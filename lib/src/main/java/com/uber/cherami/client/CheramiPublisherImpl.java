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
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.cherami.BadRequestError;
import com.uber.cherami.ChecksumOption;
import com.uber.cherami.EntityDisabledError;
import com.uber.cherami.EntityNotExistsError;
import com.uber.cherami.PutMessage;
import com.uber.cherami.ReadPublisherOptionsResult;
import com.uber.cherami.client.ConnectionManager.ConnectionFactory;
import com.uber.cherami.client.ConnectionManager.ConnectionManagerClosedException;
import com.uber.cherami.client.ConnectionManager.EndpointFinder;
import com.uber.cherami.client.ConnectionManager.EndpointsInfo;
import com.uber.cherami.client.SendReceipt.ReceiptStatus;
import com.uber.cherami.client.metrics.MetricsReporter;

/**
 * An implementation of the CheramiPublisher interface.
 */
public class CheramiPublisherImpl implements CheramiPublisher, Reconfigurable {

    private static final Logger logger = LoggerFactory.getLogger(CheramiPublisherImpl.class);

    private static final Error RECEIPT_ERROR_NO_CONNS = new Error("No conns available to publish message");
    private static final Error RECEIPT_ERROR_BUFFER_FULL = new Error("Client send buffer full");
    private static final Error RECEIPT_ERROR_CLIENT_TIMED_OUT = new Error("Client timed out.");

    private static final String OPEN_PUBLISHER_ENDPOINT = "open_publisher_stream";

    private static final int CHERAMI_INPUTHOST_SERVER_PORT = 6189;

    private final String path;
    private final long msgIdPrefix;
    private final AtomicLong msgCounter;
    private final AtomicBoolean isOpen;

    private final CheramiClient client;
    private final PublisherOptions options;
    private final MetricsReporter metricsReporter;
    private final SendBufferQueue messageQueue;
    private final ConnectionManager<InputHostConnection> connectionManager;

    /**
     * Creates and returns a CheramiPublisherImpl object.
     *
     * @param client
     *            The client that created the Publisher
     * @param path
     *            The destination path to write to
     * @param options
     *            PublisherOptions containing the tunable params.
     */
    public CheramiPublisherImpl(CheramiClient client, String path, PublisherOptions options, MetricsReporter reporter) {
        this.msgCounter = new AtomicLong(0);
        this.msgIdPrefix = new Random().nextLong();
        this.path = path;
        this.client = client;
        this.options = options;
        this.metricsReporter = reporter;
        this.messageQueue = new SendBufferQueue(options.sndBufSizeBytes);
        this.connectionManager = newConnectionManager();
        this.isOpen = new AtomicBoolean(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void open() throws InterruptedException, IOException {
        if (!isOpen.get()) {
            connectionManager.open();
            isOpen.set(true);
            logger.info("Publisher opened");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void close() {
        if (isOpen.getAndSet(false)) {
            connectionManager.close();
            while (true) {
                // drain the send buffer queue
                PutMessageRequest msg = messageQueue.poll();
                if (msg == null) {
                    break;
                }
                CheramiFuture<SendReceipt> future = msg.getAckFuture();
                String msgID = msg.getMessage().getId();
                future.setReply(new SendReceipt(msgID, ReceiptStatus.ERR_NO_CONNS, RECEIPT_ERROR_NO_CONNS));
            }
        }
        logger.info("Publisher closed");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SendReceipt write(PublisherMessage message) throws InterruptedException, IOException {
        Future<SendReceipt> future = writeAsync(message);
        try {
            return future.get(options.writeTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return new SendReceipt("", ReceiptStatus.ERR_TIMED_OUT, RECEIPT_ERROR_CLIENT_TIMED_OUT);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CheramiFuture<SendReceipt> writeAsync(PublisherMessage message) throws IOException {

        if (!isOpen()) {
            throw new IOException("Publisher is not open");
        }

        long currentCounter = msgCounter.getAndIncrement();
        String idStr = String.format("%d:%d", msgIdPrefix, currentCounter);
        PutMessageRequest request = createMessageRequest(message, idStr);
        CheramiFuture<SendReceipt> future = request.getAckFuture();

        if (getAvailableConnections().size() == 0) {
            future.setReply(new SendReceipt(idStr, ReceiptStatus.ERR_NO_CONNS, RECEIPT_ERROR_NO_CONNS));
            return future;
        }
        if (!messageQueue.offer(request)) {
            // Don't enqueue, return false indicating that buffer is full
            future.setReply(new SendReceipt(idStr, ReceiptStatus.ERR_SND_BUF_FULL, RECEIPT_ERROR_BUFFER_FULL));
            return future;
        }

        return future;
    }

    /**
     * A callback from the InputHostConnection to instruct the publisher to
     * reconfigure
     */
    @Override
    public void refreshNow() {
        logger.info("Reconfiguration command received from host connection.");
        if (!isOpen()) {
            logger.warn("Publisher is not open, dropping reconfig request.");
        }
        connectionManager.refreshNow();
    }

    /**
     * @return True if the publisher is open.
     */
    protected boolean isOpen() {
        return isOpen.get();
    }

    private PutMessageRequest createMessageRequest(PublisherMessage message, String idStr) {
        int delayInSeconds = message.getDelaySeconds();
        PutMessage msg = new PutMessage();
        msg.setId(idStr);
        msg.setData(message.getData());
        msg.setDelayMessageInSeconds(delayInSeconds);
        return new PutMessageRequest(msg, new CheramiFuture<SendReceipt>());
    }

    private EndpointsInfo findPublishEndpoints() throws IOException {
        try {
            ReadPublisherOptionsResult result = client.readPublisherOptions(path);
            return new EndpointsInfo(result.getHostAddresses(), result.getChecksumOption());
        } catch (EntityNotExistsError | EntityDisabledError e) {
            throw new IllegalStateException(e);
        } catch (BadRequestError e) {
            // should never happen unless the service side
            // broke the contract due a bad deployment, retrying
            // is the best option here
            logger.error("readPublisherOptions unexpected error", e);
            throw new IOException(e);
        }
    }

    /**
     * Use round robin to distribute the messages evenly across the InputHosts
     * by hashing the id to the host index (id % map.size()).
     *
     * @param currentCounter The ID counter of the messageID.
     * @return the InputHostConnection to send the message to. Returns null if there are no InputHostConnections
     */
    private List<InputHostConnection> getAvailableConnections() {
        try {
            return connectionManager.getConnections();
        } catch (ConnectionManagerClosedException e) {
            close();
            throw new IllegalStateException(e);
        }
    }

    private ConnectionManager<InputHostConnection> newConnectionManager() {
        ConnectionFactory<InputHostConnection> factory = new ConnectionFactory<InputHostConnection>() {
            @Override
            public InputHostConnection create(String host, int port, ChecksumOption checksumOption)
                    throws IOException, InterruptedException {
                return newInputHostConnection(host, port, checksumOption);
            }
        };
        EndpointFinder finder = new EndpointFinder() {
            @Override
            public EndpointsInfo find() throws IOException {
                return findPublishEndpoints();
            }
        };
        return new ConnectionManager<InputHostConnection>("publisher", factory, finder);
    }

    private InputHostConnection newInputHostConnection(String host, int port, ChecksumOption checksumOption)
            throws IOException, InterruptedException {
        // TODO: This will be gone once the server side is updated to not
        // support TChannel streaming at all, which means
        // the only port will be for websockets
        if (port == 4240) {
            port = CHERAMI_INPUTHOST_SERVER_PORT;
        }
        String serverUrl = String.format("ws://%s:%d/%s", host, port, OPEN_PUBLISHER_ENDPOINT);
        InputHostConnection connection = new InputHostConnection(serverUrl, path, checksumOption, options, this,
                messageQueue, metricsReporter);
        connection.open();
        return connection;
    }
}
