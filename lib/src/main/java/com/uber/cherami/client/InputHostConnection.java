/*******************************************************************************
 *  Copyright (c) 2017 Uber Technologies, Inc.
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 *******************************************************************************/
package com.uber.cherami.client;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.eclipse.jetty.io.SelectorManager;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.cherami.ChecksumOption;
import com.uber.cherami.InputHostCommand;
import com.uber.cherami.PutMessage;
import com.uber.cherami.PutMessageAck;
import com.uber.cherami.client.ConnectionManager.Connection;
import com.uber.cherami.client.SendReceipt.ReceiptStatus;
import com.uber.cherami.client.metrics.MetricsReporter;
import com.uber.cherami.client.metrics.MetricsReporter.Counter;
import com.uber.cherami.client.metrics.MetricsReporter.Latency;
import com.uber.cherami.client.metrics.MetricsReporter.LatencyProfiler;

/**
 * InputHostConnection is the connection between the publisher and an input host
 */
public class InputHostConnection implements Connection, WebsocketConnection, Runnable {

    private static final Logger logger = LoggerFactory.getLogger(InputHostConnection.class);

    private static final Error RECEIPT_ERROR_CONN_CLOSED = new Error("Stream closed");
    private static final Error RECEIPT_ERROR_TIMEDOUT = new Error("Timed out");

    /**
     * Make our timeout greater than jetty's internal connect timeout, to avoid
     * the possibility that a connection succeeds after our timeout and we leave
     * behind a zombie connection.
     */
    private static final long DEFAULT_CONNECT_TIMEOUT_SECONDS = TimeUnit.MILLISECONDS
            .toSeconds(SelectorManager.DEFAULT_CONNECT_TIMEOUT) + 1;

    private final String uri;
    private final String path;

    private final AtomicBoolean isOpen;
    private final AtomicInteger numInFlight;

    private final ConcurrentLinkedQueue<PutMessageAck> ackedMessageQueue;
    /**
     * Queue of messages to be processed.
     */
    private final SendBufferQueue messageQueue;
    /**
     * A LinkedHashMap of msgIds to CheramiFuture objects representing messages
     * that are in flight
     */
    private final LinkedHashMap<String, CheramiFuture<SendReceipt>> inFlightMessages;

    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private final CountDownLatch stoppedLatch = new CountDownLatch(1);

    private final Reconfigurable reconfigurable;
    private final CheramiClientSocket socket;
    private final PublisherOptions publisherOptions;
    private final ChecksumWriter checksumWriter;
    private final MetricsReporter metricsReporter;

    /**
     * Create an InputHostConnection.
     *
     * @param uri
     *            The websocket URI of the InputHost server
     * @param path
     *            The path of the destination to write to
     * @param checksumOption
     *            Type of checksum to be used for publishing.
     * @param publisherOptions
     *            Tunables for this connection.
     * @param reconfigurable
     *            Reconfigurable, callback for handling reconfigure commands.
     * @param msgQueue
     *            MessageQueue containing the messages to be published.
     * @param reporter
     *            MetricsReporter for reporting metrics.
     */
    public InputHostConnection(String uri, String path, ChecksumOption checksumOption,
            PublisherOptions publisherOptions, Reconfigurable reconfigurable, SendBufferQueue msgQueue,
            MetricsReporter reporter)
            throws IOException {
        this.uri = uri;
        this.path = path;
        this.publisherOptions = publisherOptions;
        this.reconfigurable = reconfigurable;
        this.numInFlight = new AtomicInteger(0);
        this.socket = new CheramiClientSocket(this, true);
        this.messageQueue = msgQueue;
        this.ackedMessageQueue = new ConcurrentLinkedQueue<>();
        this.inFlightMessages = new LinkedHashMap<>();
        this.isOpen = new AtomicBoolean(false);
        this.checksumWriter = ChecksumWriter.create(checksumOption);
        this.metricsReporter = reporter;
    }

    /**
     * Opens the connection to input host.
     *
     * @throws IOException
     *             On an I/O error.
     * @throws InterruptedException
     *             If the thread is interrupted.
     */
    public synchronized void open() throws IOException, InterruptedException {
        try {
            ClientUpgradeRequest request = new ClientUpgradeRequest();
            request.setHeader("path", path);
            URI destination = new URI(uri);
            WebSocketClient wsClient = CheramiClientImpl.getWebsocketClient();
            wsClient.connect(socket, destination, request);
            if (!socket.awaitConnect(DEFAULT_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new TimeoutException("Connection timed out");
            }
            new Thread(this).start();
            isOpen.set(true);
            logger.info("Input host connection to {} open", uri);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Closes the InputHostConnection.
     *
     * The shutdown process has 3 steps:
     *
     * <pre>
     *
     *   (1) Stop new writes to the server
     *   (2) Drain all acks (or timeout)
     *   (3) Close the socket
     *
     * </pre>
     */
    @Override
    public void close() {
        if (isOpen.getAndSet(false)) {
            try {
                countDownLatch.countDown();
                if (stoppedLatch.await(1, TimeUnit.SECONDS)) {
                    // if the thread stopped successfully, then
                    // do a graceful drain of the ack queue
                    drainAcks(publisherOptions.writeTimeoutMillis);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                failAllInflight();
                socket.close();
            }
            logger.info("Input host connection to {} closed", uri);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void receiveMessage(byte[] message) {
        try {
            TListDeserializer<InputHostCommand> deserializer = new TListDeserializer<InputHostCommand>(
                    new TBinaryProtocol.Factory());
            List<InputHostCommand> commands = deserializer.deserialize(InputHostCommand.class, message);
            for (InputHostCommand command : commands) {
                switch (command.getType()) {
                case ACK:
                    PutMessageAck ack = command.getAck();
                    ackedMessageQueue.add(ack);
                    metricsReporter.report(Counter.PUBLISHER_ACKS_IN, 1L);
                    break;
                case RECONFIGURE:
                    metricsReporter.report(Counter.PUBLISHER_RECONFIGS, 1L);
                    reconfigurable.refreshNow();
                    logger.debug("Reconfiguration command received from input host, reconfigID={}",
                            command.getReconfigure().getUpdateUUID());
                    break;
                case DRAIN:
                    // spin up a thread to drain/close the connection
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            disconnected();
                        }
                    }).start();
                    logger.debug("Drain command received from input host, reconfigID={}",
                            command.getReconfigure().getUpdateUUID());
                    break;
                }
            }
        } catch (Exception e) {
            // Can ignore message and wait for re-delivery
            logger.error("Error deserializing message", e);
        }
    }

    /**
     * @param ack
     * @return A SendReceipt object based on the PutMessageAck
     */
    private SendReceipt processAck(PutMessageAck ack) {
        SendReceipt receipt = null;
        switch (ack.getStatus()) {
        case OK:
            receipt = new SendReceipt(ack.getId(), ack.getReceipt());
            break;
        case THROTTLED:
            receipt = new SendReceipt(ack.getId(), ReceiptStatus.ERR_THROTTLED, new Error(ack.getMessage()));
            break;
        case TIMEDOUT:
            receipt = new SendReceipt(ack.getId(), ReceiptStatus.ERR_TIMED_OUT, new Error(ack.getMessage()));
            break;
        default:
            receipt = new SendReceipt(ack.getId(), ReceiptStatus.ERR_SERVICE, new Error(ack.getMessage()));
            break;
        }
        return receipt;
    }

    /**
     * Waits for the send/receive thread to finish, then returns failure for any messages left in the queue and inflight.
     */
    public void failAllInflight() {
        for (String msgID : inFlightMessages.keySet()) {
            SendReceipt receipt = new SendReceipt(msgID, ReceiptStatus.ERR_CONN_CLOSED, RECEIPT_ERROR_CONN_CLOSED);
            inFlightMessages.get(msgID).setReply(receipt);
        }
        inFlightMessages.clear();
        numInFlight.set(0);
    }

    /**
     * A callback from the socket connection to indicate that the socket connection has been disconnected.
     */
    @Override
    public void disconnected() {
        if (isOpen()) {
            reconfigurable.refreshNow();
            close();
        }
    }

    @Override
    public boolean isOpen() {
        return isOpen.get();
    }

    private void awaitMillis(long millis) {
        try {
            countDownLatch.await(millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return;
        }
    }

    /**
     * Waits for acks for the inflight messages until the given timeout and
     * returns as soon as the inflight count goes to zero.
     *
     * @param timeoutMillis
     *            long representing the timeout in milliseconds
     */
    private void drainAcks(long timeoutMillis) {
        long expiryMillis = System.currentTimeMillis() + timeoutMillis;
        while (!inFlightMessages.isEmpty()) {
            long now = System.currentTimeMillis();
            if (now >= expiryMillis) {
                return;
            }
            if (ackedMessageQueue.isEmpty()) {
                awaitMillis(5);
                continue;
            }
            readProcessAcks(Integer.MAX_VALUE);
        }
    }

    private void readProcessAcks(int limit) {
        // Ack batch size messages if you can
        int numToAck = Math.min(ackedMessageQueue.size(), limit);
        for (int i = 0; i < numToAck; i++) {
            PutMessageAck ack = ackedMessageQueue.poll();
            String ackId = ack.getId();
            if (inFlightMessages.containsKey(ackId)) {
                SendReceipt done = processAck(ack);
                inFlightMessages.remove(ackId).setReply(done);
                numInFlight.getAndDecrement();
            } else {
                logger.debug("Ignoring delayed ackId: {} Status: {}", ackId, ack.getStatus());
            }
        }
    }

    /**
     * If messageQueue, ackedMessageQueue, and inFlightMessages are all empty, just sleep until there is work to do.
     * If there are messages in the queue and inFlightMessageSize < maxInflight, remove the first message from the queue
     * and add it to inFlight
     * <p>
     * If the ackedMessageQueue and messageQueue is empty, check if there are any inflight messages.
     * If there are, check to see if the oldest message has timed out.
     * If not, check when it will timeout and sleep until it times out or ackedMessageQueue or messageQueue becomes not empty
     * If the oldest message has timed out, set the corresponding future to FAILED and remove from inFlight.
     * <p>
     * However, if there are any acks in the ackedMessageQueue, try to process two acks if possible.
     * After processing the acks, set the corresponding future to success and remove from inFlight.
     */
    @Override
    public void run() {

        PutMessageRequest request = null;
        final long writeTimeoutMillis = publisherOptions.writeTimeoutMillis;
        LatencyProfiler profiler = metricsReporter.createLatencyProfiler(Latency.PUBLISH_MESSAGE);

        try {
            while (countDownLatch.getCount() > 0) {
                try {

                    while (countDownLatch.getCount() > 0 && messageQueue.isEmpty() && ackedMessageQueue.isEmpty()
                            && inFlightMessages.isEmpty()) {
                        countDownLatch.await(5, TimeUnit.MILLISECONDS);
                    }
                    int maxInFlightMsgs = publisherOptions.inFlightMsgsPerConn;
                    if (!messageQueue.isEmpty() && numInFlight.get() < maxInFlightMsgs) {
                        // Get first element (oldest message) in messageQueue
                        request = messageQueue.poll();
                        if (request != null) {
                            PutMessage message = request.getMessage();
                            checksumWriter.write(message);
                            List<PutMessage> payload = new ArrayList<>();
                            payload.add(message);

                            metricsReporter.report(Counter.PUBLISHER_MESSAGES_OUT, 1L);
                            TListSerializer<PutMessage> serializer = new TListSerializer<PutMessage>(
                                    new TBinaryProtocol.Factory());
                            try {
                                profiler.start();
                                byte[] bytes = serializer.serialize(payload);
                                socket.sendMessage(bytes);
                                inFlightMessages.put(message.getId(), request.getAckFuture());
                                numInFlight.getAndIncrement();
                            } catch (Exception e) {
                                // Error in sending message, set future to fail
                                logger.warn("Error writing message id {} to stream", message.getId(), e);
                                SendReceipt err = new SendReceipt(message.getId(), ReceiptStatus.ERR_SOCKET,
                                        new Error(e));
                                request.getAckFuture().setReply(err);
                                metricsReporter.report(Counter.PUBLISHER_MESSAGES_OUT_FAILED, 1L);
                            } finally {
                                profiler.end();
                                profiler.reset();
                            }
                        }
                    }
                    if (ackedMessageQueue.isEmpty() && !inFlightMessages.isEmpty()) {
                        // no acks pending and we have atleast one entry in
                        // inflight map, lets process some timeouts
                        long sleepMillis = 0;
                        do {
                            if (sleepMillis > 0) {
                                countDownLatch.await(sleepMillis, TimeUnit.MILLISECONDS);
                                if (countDownLatch.getCount() < 1) {
                                    return;
                                }
                                if (!ackedMessageQueue.isEmpty()) {
                                    break;
                                }
                            }

                            long nowMillis = System.currentTimeMillis();
                            String id = inFlightMessages.keySet().iterator().next();
                            CheramiFuture<SendReceipt> oldestFuture = inFlightMessages.get(id);
                            long expiryMillis = oldestFuture.getSentTime().getTime() + writeTimeoutMillis;
                            if ((expiryMillis - nowMillis) < 0) {
                                // Message has timed out, set future to fail and
                                // remove
                                // from inFlight.
                                SendReceipt receipt = new SendReceipt(id, ReceiptStatus.ERR_TIMED_OUT,
                                        RECEIPT_ERROR_TIMEDOUT);
                                inFlightMessages.remove(id).setReply(receipt);
                                numInFlight.getAndDecrement();
                                metricsReporter.report(Counter.PUBLISHER_MESSAGES_OUT_FAILED, 1L);
                                sleepMillis = 0;
                                continue;
                            }

                            sleepMillis = Math.min(1, (expiryMillis - nowMillis));
                            sleepMillis = Math.max(5, sleepMillis);

                        } while (countDownLatch.getCount() > 0 && messageQueue.isEmpty() && ackedMessageQueue.isEmpty()
                                && !inFlightMessages.isEmpty());
                    }

                    if (!ackedMessageQueue.isEmpty()) {
                        // process 2 acks for every message sent
                        readProcessAcks(2);
                    }

                } catch (InterruptedException e) {
                    // If internal thread was interrupted, just reset
                    // interrupted flag and ignore
                }
            }
        } catch (Throwable throwable) {
            logger.error("InputHostConnection caught unexpected exception", throwable);
            stoppedLatch.countDown();
            close();
        } finally {
            stoppedLatch.countDown();
        }
    }
}
