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
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Websocket for the Client. Supports handlers for connecting, receiving a binary message, and closing
 */
@WebSocket
public class CheramiClientSocket {
    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 10;
    private static final int KEEP_ALIVE_INTERVAL_SECONDS = 60;
    private final Logger logger = LoggerFactory.getLogger(CheramiClientSocket.class);
    private final CountDownLatch countDownLatch;
    private final WebsocketConnection connection;
    private final boolean keepAlive;
    private Session session;
    private ScheduledExecutorService exec;

    /**
     * Constructor for CheramiClientSocket.
     *
     * @param connection
     *            WebSocketConnection object.
     * @param keepAlive
     *            True if keepAlive must be enabled.
     */
    public CheramiClientSocket(WebsocketConnection connection, boolean keepAlive) {
        this.connection = connection;
        this.keepAlive = keepAlive;
        countDownLatch = new CountDownLatch(1);
    }

    /**
     * Callback that gets invoked when the underlying connection is established.
     */
    @OnWebSocketConnect
    public void onConnect(final Session session) {
        logger.info("Connected to server {}", session.getRemoteAddress());
        this.session = session;
        this.session.getPolicy().setMaxBinaryMessageSize(Integer.MAX_VALUE);
        this.session.getPolicy().setIdleTimeout(Long.MAX_VALUE);
        countDownLatch.countDown();
        exec = Executors.newSingleThreadScheduledExecutor();
        if (keepAlive) {
            exec.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    byte[] pingPayload = {};
                    try {
                        session.getRemote().sendPing(ByteBuffer.wrap(pingPayload));
                    } catch (IOException e) {
                        logger.error("Error sending Ping message", e);
                    }
                }
            }, 0, KEEP_ALIVE_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
    }

    /**
     * Sends a byte array. This call is blocking and will not return until the send has completed or thrown exception
     *
     * @param msg The bytes to be sent
     * @throws IOException
     */
    public void sendMessage(byte[] msg) throws IOException {
        session.getRemote().sendBytes(ByteBuffer.wrap(msg));
    }

    @OnWebSocketMessage
    public void processUpload(Session session, byte[] input, int arg1, int arg2) {
        connection.receiveMessage(input);
    }

    /**
     * Callback that's invoked when the underlying connection is closed.
     */
    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
        try {
            logger.info("Disconnected from server {}, statusCode={}, reason={}", session.getRemoteAddress(), statusCode,
                    reason);
            connection.disconnected();
            exec.shutdown();
            exec.awaitTermination(DEFAULT_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            this.session = null;
        } catch (InterruptedException e) {
            logger.error("Error stopping socket", e);
        }
    }

    /**
     * Blocks until the connect succeeds or it times out
     *
     * @param timeout The number of seconds to wait for countDownLatch to reach 0
     * @return TRUE if countDownLatch reached 0, FALSE if it timed out before it could reach 0.
     * @throws InterruptedException
     */
    public boolean awaitConnect(long timeout, TimeUnit timeunit) throws InterruptedException {
        return countDownLatch.await(timeout, timeunit);
    }
}
