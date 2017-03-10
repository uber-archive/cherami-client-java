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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.cherami.BadRequestError;
import com.uber.cherami.ChecksumOption;
import com.uber.cherami.EntityDisabledError;
import com.uber.cherami.EntityNotExistsError;
import com.uber.cherami.HostAddress;
import com.uber.cherami.client.ConnectionManager.ConnectionFactory;
import com.uber.cherami.client.ConnectionManager.ConnectionManagerClosedException;
import com.uber.cherami.client.ConnectionManager.EndpointFinder;
import com.uber.cherami.client.ConnectionManager.EndpointsInfo;
import com.uber.cherami.client.metrics.MetricsReporter;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;

/**
 * Implementation of CheramiConsumer to receive messages from Cherami service
 */
public class CheramiConsumerImpl implements CheramiConsumer, Reconfigurable {

    private static final Logger logger = LoggerFactory.getLogger(CheramiConsumerImpl.class);

    private static final String DELIVERY_TOKEN_SPLITTER = "|";
    private static final String OUTPUT_SERVICE_NAME = "cherami-outputhost";
    private static final String OPEN_CONSUMER_API = "open_consumer_stream";

    private static final int TCHANNEL_STREAMING_PORT = 4254;
    private static final int WEBSOCKET_STREAMING_PORT = 6190;

    private final String path;
    private final String consumerGroupName;

    private final AtomicBoolean isOpen;
    private final ArrayBlockingQueue<CheramiDelivery> deliveryQueue;
    /**
     * futuresQueue represents the pending CheramiFutures that have been
     * returned to the consumer but have not had their responses set yet.
     */
    private final LinkedBlockingQueue<CheramiFuture<CheramiDelivery>> futuresQueue;
    private final ConnectionManager<OutputHostConnection> connectionManager;

    private TChannel tChannel;
    private SubChannel subChannel;

    private final ConsumerOptions options;
    private final CheramiClient client;
    private final DeliveryRunnable deliveryRunnable;
    private final MetricsReporter metricsReporter;

    /**
     * Creates a CheramiConsumerImpl object.
     *
     * @param client
     *            The Client object that created the Consumer
     * @param dstPath
     *            The path of the destination to consumer from
     * @param consumerGroupName
     *            The name of the ConsumerGroup
     * @param options
     *            Consumer configurable options
     * @param metricsReporter
     *            MetricsReporter for reporting metrics.
     */
    public CheramiConsumerImpl(CheramiClient client, String dstPath, String consumerGroupName, ConsumerOptions options,
            MetricsReporter metricsReporter) {
        this.client = client;
        this.path = dstPath;
        this.consumerGroupName = consumerGroupName;
        this.options = options;
        this.metricsReporter = metricsReporter;
        this.isOpen = new AtomicBoolean(false);
        this.deliveryRunnable = new DeliveryRunnable();
        this.connectionManager = newConnectionManager();
        this.futuresQueue = new LinkedBlockingQueue<>();
        this.deliveryQueue = new ArrayBlockingQueue<>(options.prefetchSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void open() throws InterruptedException, IOException {
        if (!isOpen()) {
            String clientName = this.client.getOptions().getClientAppName();
            this.tChannel = new TChannel.Builder(String.format("%s_%s", clientName, consumerGroupName)).build();
            this.subChannel = tChannel.makeSubChannel(OUTPUT_SERVICE_NAME);
            // open connmanager only after creating subchannel
            connectionManager.open();
            new Thread(deliveryRunnable).start();
            isOpen.set(true);
            logger.info("Consumer opened.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void close() {
        if (isOpen.getAndSet(false)) {
            connectionManager.close();
            deliveryRunnable.stop();
            cleanup();
        }
        logger.info("Consumer closed");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CheramiDelivery read() throws InterruptedException, IOException {
        if (!isOpen()) {
            throw new IOException("Consumer closed.");
        }
        CheramiFuture<CheramiDelivery> future = readAsync();
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CheramiFuture<CheramiDelivery> readAsync() throws IOException {
        if (!isOpen()) {
            throw new IOException("Consumer closed.");
        }
        CheramiFuture<CheramiDelivery> future = new CheramiFuture<>();
        futuresQueue.add(future);
        return future;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ackDelivery(String deliveryToken) throws IOException {
        Pair<OutputHostConnection, DeliveryID> result = getAcknowledger(deliveryToken);
        if (result == null) {
            logger.warn("Dropping ack, connection no longer exist, token={}.", deliveryToken);
            return;
        }
        OutputHostConnection acknowledger = result.getFirst();
        DeliveryID id = result.getSecond();
        acknowledger.ack(id.getMessageAckID());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void nackDelivery(String deliveryToken) throws IOException {
        Pair<OutputHostConnection, DeliveryID> result = getAcknowledger(deliveryToken);
        if (result == null) {
            logger.warn("Dropping nack, connection no longer exist, token={}.", deliveryToken);
            return;
        }
        OutputHostConnection acknowledger = result.getFirst();
        DeliveryID id = result.getSecond();
        acknowledger.nack(id.getMessageAckID());
    }

    public boolean isOpen() {
        return isOpen.get();
    }

    /**
     * A callback from the OutputHostConnection to instruct the Consumer to
     * reconfigure
     */
    @Override
    public void refreshNow() {
        logger.info("Reconfiguration command received from consumer connection.");
        if (!isOpen()) {
            logger.warn("Consumer is not open. Ignore reconfiguration.");
            return;
        }
        connectionManager.refreshNow();
    }

    /**
     * Cleanup closes all the OutputHostConnections, shuts down the tChannel and sets all pending futures to failure
     * Calling this method multiple times has no side effects.
     */
    private void cleanup() {
        tChannel.shutdown();
        Exception err = new IllegalStateException("Consumer closed.");
        // Set the responses of all pending futures to failure
        for (CheramiFuture<CheramiDelivery> future : futuresQueue) {
            future.setError(err);
        }
    }

    private Pair<OutputHostConnection, DeliveryID> getAcknowledger(String token)  {
        DeliveryID id = new DeliveryID(token);
        Map<String, OutputHostConnection> connMap;
        try {
            connMap = connectionManager.getAddrToConnectionMap();
        } catch (ConnectionManagerClosedException e) {
            throw new IllegalStateException(e);
        }
        if (!connMap.containsKey(id.getAcknowledgerID())) {
            return null;
        }
        return new Pair<OutputHostConnection, DeliveryID>(connMap.get(id.getAcknowledgerID()), id);
    }

    private EndpointsInfo findConsumeEndpoints() throws IOException {
        try {
            List<HostAddress> hostAddrs = client.readConsumerGroupHosts(path, consumerGroupName);
            return new EndpointsInfo(hostAddrs, ChecksumOption.CRC32IEEE);
        } catch (EntityNotExistsError | EntityDisabledError e) {
            throw new IllegalStateException(e);
        } catch (BadRequestError e) {
            // should never happen unless the service side
            // broke the contract due a bad deployment, retrying
            // is the best option here
            logger.error("readConsumerGroupHosts unexpected error", e);
            throw new IOException(e);
        }
    }

    private OutputHostConnection newOutputHostConnection(String host, int port, ChecksumOption checksumOption)
            throws IOException, InterruptedException {
        // TODO: This will be gone once the server side is updated to not
        // support TChannel streaming at all, which means
        // the only port will be for websockets
        int dataPort = (port == TCHANNEL_STREAMING_PORT) ? WEBSOCKET_STREAMING_PORT : port;
        String wsUrl = String.format("ws://%s:%d/%s", host, dataPort, OPEN_CONSUMER_API);
        OutputHostConnection connection = new OutputHostConnection(wsUrl, host, port, path, consumerGroupName,
                subChannel, options, this, deliveryQueue, metricsReporter);
        connection.open();
        return connection;
    }

    private ConnectionManager<OutputHostConnection> newConnectionManager() {
        ConnectionFactory<OutputHostConnection> factory = new ConnectionFactory<OutputHostConnection>() {
            @Override
            public OutputHostConnection create(String host, int port, ChecksumOption checksumOption)
                    throws IOException, InterruptedException {
                return newOutputHostConnection(host, port, checksumOption);
            }
        };
        EndpointFinder finder = new EndpointFinder() {
            @Override
            public EndpointsInfo find() throws IOException {
                return findConsumeEndpoints();
            }
        };
        return new ConnectionManager<OutputHostConnection>("consumer", factory, finder);
    }

    /**
     * A Runnable object that sets the responses of pending futures to the CheramiDelivery objects that have received.
     */
    private class DeliveryRunnable implements Runnable {

        private static final int POLL_TIMEOUT_MILLIS = 1000;

        CountDownLatch countDownLatch = new CountDownLatch(1);
        CountDownLatch stoppedLatch = new CountDownLatch(1);

        public void stop() {
            try {
                countDownLatch.countDown();
                stoppedLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void run() {
            try {
                while (countDownLatch.getCount() > 0) {
                    try {
                        CheramiFuture<CheramiDelivery> future = futuresQueue.poll(POLL_TIMEOUT_MILLIS,
                                TimeUnit.MILLISECONDS);
                        if (future == null) {
                            continue;
                        }

                        CheramiDelivery delivery = null;
                        while (delivery == null) {
                            delivery = deliveryQueue.poll(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                            if (delivery != null) {
                                future.setReply(delivery);
                                break;
                            }
                            if (countDownLatch.getCount() == 0) {
                                future.setError(new IllegalStateException("Consumer closed."));
                                break;
                            }
                        }

                    } catch (InterruptedException e) {
                    }
                }
            } catch (Throwable e) {
                logger.error("Delivery thread caught unexpected exception", e);
                stoppedLatch.countDown();
                close();
            } finally {
                stoppedLatch.countDown();
            }
        }
    }

    /**
     * An object that contains the acknowledgerID and messageAckID of a delivered message
     */
    private class DeliveryID {
        /**
         * AckowledgerID is the identifier for underlying acknowledger which can be used to Ack/Nack this delivery
         */
        private final String acknowledgerID;
        /**
         * MessageAckID is the Ack identifier for this delivery
         */
        private final String messageAckID;

        /**
         * Decodes the token which is of the form acknowledgerID|msgAckID
         *
         * @param token is generated in delivery.getDeliveryToken and represents a specific Delivery object
         */
        DeliveryID(String token) {
            String[] parts = token.split(DELIVERY_TOKEN_SPLITTER);
            if (parts.length != 2) {
                throw new IllegalArgumentException(String.format("Invalid delivery token: %s", token));
            }
            this.acknowledgerID = parts[0];
            this.messageAckID = parts[1];
        }

        String getAcknowledgerID() {
            return acknowledgerID;
        }

        String getMessageAckID() {
            return messageAckID;
        }
    }
}
