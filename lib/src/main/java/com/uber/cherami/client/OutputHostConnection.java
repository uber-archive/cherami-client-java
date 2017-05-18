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
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.eclipse.jetty.io.SelectorManager;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.uber.cherami.AckMessagesRequest;
import com.uber.cherami.BOut.ackMessages_args;
import com.uber.cherami.BOut.ackMessages_result;
import com.uber.cherami.ConsumerMessage;
import com.uber.cherami.ControlFlow;
import com.uber.cherami.OutputHostCommand;
import com.uber.cherami.client.ConnectionManager.Connection;
import com.uber.cherami.client.metrics.MetricsReporter;
import com.uber.cherami.client.metrics.MetricsReporter.Counter;
import com.uber.cherami.client.metrics.MetricsReporter.Gauge;
import com.uber.cherami.client.metrics.MetricsReporter.Latency;
import com.uber.cherami.client.metrics.MetricsReporter.LatencyProfiler;
import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;

/**
 * OutputHostConnection is the connection between the consumer and an output host
 */
public class OutputHostConnection implements WebsocketConnection, Connection, CheramiAcknowledger {

    private static final Logger logger = LoggerFactory.getLogger(OutputHostConnection.class);

    private static final String THRIFT_INTERFACE_NAME = "BOut";
    private static final String OUTPUT_SERVICE_NAME = "cherami-outputhost";

    /**
     * Make our timeout greater than jetty's internal connect timeout, to avoid
     * the possibility that a connection succeeds after our timeout and we leave
     * behind a zombie connection.
     */
    private static final long CONNECT_TIMEOUT_SECONDS = TimeUnit.MILLISECONDS
            .toSeconds(SelectorManager.DEFAULT_CONNECT_TIMEOUT) + 1;

    private final int remotePort;
    private final int creditBatchSize;

    private final String wsUrl;
    private final String dstPath;
    private final String acknowledgerID;
    private final String consumerGroupName;

    private final AtomicBoolean isOpen;
    private final InetAddress remoteInetAddr;
    private final BlockingQueue<CheramiDelivery> msgDeliveryQueue;

    private final SubChannel subChannel;
    private final ConsumerOptions options;
    private final Reconfigurable reconfigurable;
    private final CheramiClientSocket socket;
    private final WriteAcksPump writeAcksPump;
    private final ReadMessagesPump readMessagesPump;
    private final ChecksumValidator checksumValidator;
    private final MetricsReporter metricsReporter;

    /**
     * The queue to store the received messages that we haven't acked yet
     */
    private final LinkedBlockingQueue<OutputHostCommand> receivedMessages;
    /**
     * The queue to store which messages have been nacked/acked by the Consumer.
     * The type that is stored in the queue is a Pair with key type AckType representing Ack or Nack
     * and value type String representing ids.
     */
    private final LinkedBlockingQueue<Pair<AckType, String>> ackBuffer;

    /**
     * An enum to indicate whether a specific ackId belongs to an ACK or a NACK
     */
    private enum AckType {
        ACK, NACK
    }

    /**
     * Creates an OutputHostConnection object that can be used to stream data in
     * from the cherami servers.
     *
     * @param wsUrl
     *            Websocket data streaming url.
     * @param remoteAddr
     *            Remote address of the server.
     * @param remotePort
     *            Remote port that will be used to send acks/nacks.
     * @param dstPath
     *            The path of the destination to read from
     * @param consumerGroupName
     *            The name of the consumerGroup that is consuming the messages
     * @param subChannel
     *            The SubChannel to be used for Ack/Nack rpc calls.
     * @param options
     *            ConsumerOptions, tunables for the consumer.
     * @param reconfigurable
     *            Callback for reconfiguration notifications.
     * @param msgDeliveryQueue
     *            Queue where the received messages will be enqueued.
     * @param metricsReporter
     *            MetricsReport for emitting metrics.
     */
    public OutputHostConnection(String wsUrl, String remoteAddr, int remotePort, String dstPath,
            String consumerGroupName, SubChannel subChannel, ConsumerOptions options, Reconfigurable reconfigurable,
            BlockingQueue<CheramiDelivery> msgDeliveryQueue, MetricsReporter metricsReporter) {
        this.wsUrl = wsUrl;
        this.dstPath = dstPath;
        this.consumerGroupName = consumerGroupName;
        this.options = options;
        this.creditBatchSize = Math.max(1, options.prefetchSize / 10);
        this.receivedMessages = new LinkedBlockingQueue<>();
        this.ackBuffer = new LinkedBlockingQueue<>();
        this.socket = new CheramiClientSocket(this, options.keepAlive);
        this.writeAcksPump = new WriteAcksPump();
        this.readMessagesPump = new ReadMessagesPump(this);
        this.isOpen = new AtomicBoolean(false);
        this.subChannel = subChannel;
        this.checksumValidator = new ChecksumValidator();
        this.reconfigurable = reconfigurable;
        this.msgDeliveryQueue = msgDeliveryQueue;
        this.metricsReporter = metricsReporter;
        this.remotePort = remotePort;
        this.acknowledgerID = remoteAddr + ":" + remotePort;
        try {
            this.remoteInetAddr = InetAddress.getByName(remoteAddr);
        } catch (Exception e) {
            throw new RuntimeException("Invalid remote addr:" + remoteAddr);
        }
    }

    /**
     * Opens a connection to the output host.
     *
     * @throws IOException
     *             On an I/O error
     * @throws InterruptedException
     *             If the thread is interrupted
     */
    public synchronized void open() throws IOException, InterruptedException {
        if (!isOpen()) {
            try {
                ClientUpgradeRequest request = new ClientUpgradeRequest();
                request.setHeader("path", dstPath);
                request.setHeader("consumerGroupName", consumerGroupName);
                URI serverUri = new URI(wsUrl);
                WebSocketClient wsClient = CheramiClientImpl.getWebsocketClient();
                wsClient.connect(socket, serverUri, request);
                if (!socket.awaitConnect(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IOException("Connection timed out to url=" + wsUrl);
                }
                isOpen.set(true);
                new Thread(readMessagesPump).start();
                new Thread(writeAcksPump).start();
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
            logger.info("Ouput host conenction to {} open", wsUrl);
        }
    }

    /**
     * Closes the OutputHostConnection by stopping the readMessages and writeAcks pumps.
     * Calling this method multiple times has no effect
     */
    @Override
    public void close() {
        if (isOpen.getAndSet(false)) {
            readMessagesPump.stop();
            writeAcksPump.stop();
            socket.close();
            logger.info("Output host connection to {} closed", wsUrl);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void receiveMessage(byte[] message) {
        if (isOpen()) {
            try {
                TListDeserializer<OutputHostCommand> deserializer = new TListDeserializer<OutputHostCommand>(
                        new TBinaryProtocol.Factory());
                List<OutputHostCommand> commands = deserializer.deserialize(OutputHostCommand.class, message);
                int i = 0;
                while (i < commands.size() && isOpen()) {
                    try {
                        receivedMessages.put(commands.get(i));
                        i++;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            } catch (TException e) {
                logger.error("Error in deserializing message: {}", e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void disconnected() {
        try {
            closeAndNotify();
        } catch (Exception e) {
            logger.error("Error while closing OutputHost connection", e);
        }
    }

    @Override
    public String getAcknowledgerID() {
        return acknowledgerID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ack(String id) {
        if (isOpen()) {
            Pair<AckType, String> pair = new Pair<>(AckType.ACK, id);
            ackBuffer.add(pair);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void nack(String id) {
        if (isOpen()) {
            Pair<AckType, String> pair = new Pair<>(AckType.NACK, id);
            ackBuffer.add(pair);
        }
    }

    @Override
    public boolean isOpen() {
        return isOpen.get();
    }

    private void closeAndNotify() {
        close();
        reconfigurable.refreshNow();
    }

    /**
     * A runnable that pushes received messages to the Consumer via the delivery Channel.
     * If the deliveryChannel is full, it will block until space clears up.
     */
    private class ReadMessagesPump implements Runnable {

        private static final long BACKOFF_INTERVAL_MILLIS = 1000;
        private static final long MSGQ_POLL_TIMEOUT_MILLIS = 1000;
        private static final long DELIVERY_Q_POLL_TIME_MILLIS = 100;

        private final CountDownLatch quitter = new CountDownLatch(1);
        private final CheramiAcknowledger acknowledger;
        private final Random random = new Random();

        private boolean prevSendCreditsFailed = false;
        private long nextSendCreditsTime = System.currentTimeMillis();

        ReadMessagesPump(CheramiAcknowledger acknowledger) {
            this.acknowledger = acknowledger;
        }

        public void stop() {
            quitter.countDown();
        }

        /**
         * A blocking call that sends credits to the outputHost.
         *
         * @param credits The number of credits to send to the outputHost
         * @throws TException
         * @throws IOException
         */
        private void sendCredits(int credits) throws TException, IOException {
            ControlFlow flows = new ControlFlow();
            flows.setCredits(credits);
            TListSerializer<ControlFlow> serializer = new TListSerializer<ControlFlow>(new TBinaryProtocol.Factory());
            List<ControlFlow> list = new ArrayList<>();
            list.add(flows);
            byte[] bytes = serializer.serialize(list);
            socket.sendMessage(bytes);
        }

        private boolean isAllowSendCredits() {
            if (!prevSendCreditsFailed) {
                return true;
            }
            return (System.currentTimeMillis() >= nextSendCreditsTime);
        }

        private void addToDeliveryQueue(CheramiDelivery delivery) {
            while (quitter.getCount() > 0) {
                try {
                    long jitter = random.nextInt(10);
                    long pollTime = Math.max(0, MSGQ_POLL_TIMEOUT_MILLIS - jitter);
                    if (msgDeliveryQueue.offer(delivery, pollTime, TimeUnit.MILLISECONDS)) {
                        break;
                    }
                } catch (InterruptedException e) {
                }
            }
        }

        private void blockUntilDeliveryQueueFull() {
            while ((options.prefetchSize == msgDeliveryQueue.size()) && quitter.getCount() > 0) {
                try {
                    long jitter = random.nextInt(20);
                    long backoffTime = Math.max(0, DELIVERY_Q_POLL_TIME_MILLIS - jitter);
                    Thread.sleep(backoffTime);
                } catch (InterruptedException e) {
                }
            }
        }

        @Override
        public void run() {

            // Set localCredits to prefetchSize so we send initial credits to OutputHost
            int localCredits = options.prefetchSize;
            LatencyProfiler profiler = metricsReporter.createLatencyProfiler(Latency.SEND_CREDITS);
            try {
                while (quitter.getCount() > 0) {
                    try {
                        // If the prefetch buffer is full, that
                        // indicates application isn't keeping with the
                        // consumption rate. No point in granting
                        // more credits, wait until there is room
                        // in the prefetch buffer
                        blockUntilDeliveryQueueFull();
                        if (quitter.getCount() <= 0) {
                            return;
                        }

                        if (localCredits >= creditBatchSize && isAllowSendCredits()) {
                            try {
                                metricsReporter.report(Counter.CONSUMER_CREDITS_OUT, localCredits);
                                profiler.start();
                                sendCredits(localCredits);
                                localCredits = 0;
                                prevSendCreditsFailed = false;
                                metricsReporter.report(Gauge.CONSUMER_LOCAL_CREDITS, localCredits);
                            } catch (TException | IOException e) {
                                prevSendCreditsFailed = true;
                                nextSendCreditsTime = System.currentTimeMillis() + BACKOFF_INTERVAL_MILLIS;
                                logger.warn("Error sending credits to {}", acknowledgerID, e);
                                metricsReporter.report(Counter.CONSUMER_CREDITS_OUT_FAILED, localCredits);
                            } finally {
                                profiler.end();
                                profiler.reset();
                            }
                        }

                        OutputHostCommand command = receivedMessages.poll(MSGQ_POLL_TIMEOUT_MILLIS,
                                TimeUnit.MILLISECONDS);
                        if (command == null) {
                            continue;
                        }
                        switch (command.getType()) {
                        case MESSAGE:
                            ConsumerMessage msg = command.getMessage();
                            CheramiDelivery delivery = new CheramiDeliveryImpl(msg, acknowledger);
                            if (!checksumValidator.validate(msg)) {
                                logger.error("Received msg {} with invalid checksum", msg.getAckId());
                                delivery.nack();
                            }
                            addToDeliveryQueue(delivery);
                            localCredits++;
                            metricsReporter.report(Counter.CONSUMER_MESSAGES_IN, 1L);
                            metricsReporter.report(Gauge.CONSUMER_LOCAL_CREDITS, localCredits);
                            break;
                        case RECONFIGURE:
                            metricsReporter.report(Counter.CONSUMER_RECONFIGS, 1L);
                            logger.info("Reconfiguration command received from {}.", acknowledgerID);
                            reconfigurable.refreshNow();
                            break;
                        case END_OF_STREAM:
                            closeAndNotify();
                            return;
                        }
                    } catch (InterruptedException e) {
                        // Don't want to quit loop if interrupted during enqueue
                        // or polling receivedMessages
                        Thread.currentThread().interrupt();
                    }
                }
            } catch (Throwable throwable) {
                logger.error("ReadMessagesPump caught unexpected exception, remote={}", acknowledgerID, throwable);
                metricsReporter.report(Counter.CONSUMER_MESSAGES_IN_FAILED, 1L);
                closeAndNotify();
            }
        }
    }

    /**
     * A runnable that takes the message acks it receives from the Consumer and writes them in batches to the OutputHost.
     */
    private class WriteAcksPump implements Runnable {

        private static final long SEND_ACK_TIMEOUT_MILLIS = 30 * 1000;
        private static final long BATCH_TIMEOUT_MILLIS = 100;
        /**
         * ackIds are about 133 bytes each and MTU is about 1500, so try to keep
         * it to one packet full
         */
        private static final int ACK_BATCH_SIZE = 9;

        private long nextSendAckTime = System.currentTimeMillis();

        private final CountDownLatch quitter = new CountDownLatch(1);
        private final CountDownLatch stoppedLatch = new CountDownLatch(1);
        private List<Pair<AckType, String>> buffer = new ArrayList<>();

        public void stop() {
            try {
                quitter.countDown();
                stoppedLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        /**
         * Initiates RPC call to forward the acks to output host.
         *
         * @return Boolean, if this method returns false, caller must retry.
         */
        private void pushAcks(AckMessagesRequest ackRequest) {

            ThriftResponse<ackMessages_result> response = null;
            try {

                String endpoint = String.format("%s::%s", THRIFT_INTERFACE_NAME, "ackMessages");

                ThriftRequest.Builder<ackMessages_args> builder = new ThriftRequest.Builder<ackMessages_args>(
                        OUTPUT_SERVICE_NAME, endpoint);
                builder.setTimeout(SEND_ACK_TIMEOUT_MILLIS);
                builder.setBody(new ackMessages_args(ackRequest));

                // We need to specify the specific host port to send the acks to
                // because all outputHostConnections use the same subChannel
                // object, so the subChannel contains peers for all
                // outputHostConnections. Throws TChannelError if the message
                // cannot be sent
                ListenableFuture<ThriftResponse<ackMessages_result>> future = subChannel.send(builder.build(),
                        remoteInetAddr, remotePort);
                response = future.get();
                if (response == null || response.isError() || response.getResponseCode() != ResponseCode.OK) {
                    metricsReporter.report(Counter.CONSUMER_ACKS_OUT_FAILED, 1L);
                }

            } catch (Exception e) {
                metricsReporter.report(Counter.CONSUMER_ACKS_OUT_FAILED, 1L);
                logger.warn("Error sending acks, remote={}", acknowledgerID, e);
            } finally {
                if (response != null) {
                    response.release();
                }
            }
        }

        private void sendAckRequest(List<Pair<AckType, String>> buffer) {
            AckMessagesRequest ackRequest = new AckMessagesRequest();
            ArrayList<String> ackIds = new ArrayList<>();
            ArrayList<String> nackIds = new ArrayList<>();
            for (Pair<AckType, String> pair : buffer) {
                if (pair.getFirst() == AckType.ACK) {
                    ackIds.add(pair.getSecond());
                    metricsReporter.report(Counter.CONSUMER_ACKS_OUT, 1L);
                } else {
                    nackIds.add(pair.getSecond());
                    metricsReporter.report(Counter.CONSUMER_NACKS_OUT, 1L);
                }
            }
            ackRequest.setAckIds(ackIds);
            ackRequest.setNackIds(nackIds);
            pushAcks(ackRequest);
        }

        private boolean isSendAckDue() {
            if (buffer.isEmpty()) {
                return false;
            }
            return (buffer.size() >= ACK_BATCH_SIZE || System.currentTimeMillis() >= nextSendAckTime);
        }

        @Override
        public void run() {
            try {
                while (quitter.getCount() > 0) {
                    try {
                        if (isSendAckDue()) {
                            sendAckRequest(buffer);
                            buffer.clear();
                            nextSendAckTime = System.currentTimeMillis() + BATCH_TIMEOUT_MILLIS;
                        }
                        Pair<AckType, String> tuple = ackBuffer.poll(BATCH_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                        if (tuple != null) {
                            buffer.add(tuple);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                // If there are unacked messages, try to push them all before
                // bailing out of the pump
                if (!buffer.isEmpty()) {
                    sendAckRequest(buffer);
                    buffer.clear();
                }

            } catch (Throwable throwable) {
                logger.error("WriteAcksPump caught unexpected exception, remote={}", acknowledgerID, throwable);
                stoppedLatch.countDown();
                closeAndNotify();
            } finally {
                stoppedLatch.countDown();
            }
        }
    }
}
