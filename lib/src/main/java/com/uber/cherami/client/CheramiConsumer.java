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
import java.util.concurrent.Future;

/**
 * Consumer that can receive messages from cherami.
 */
public interface CheramiConsumer {
    /**
     * Initiates opening of underlying streams for consuming data.
     *
     * This method initiates creation of streams / connections to cherami
     * servers for consuming data. If the destination or consumer group is
     * deleted or no longer exists, this method will throw an exception. This
     * method *does not* throw any exception on intermittent i/o or discovery
     * errors. It automatically runs the backoff / retry/ reconfig protocols in
     * the background for recovering from intermittent errors.
     *
     * @throws IOException
     *             Only on unrecoverable errors.
     */
    void open() throws InterruptedException, IOException;

    /**
     * Closes all the underlying consumer streams.
     *
     * All pending futures will receive an ExecutionException with the cause
     * being connection closed. Any blocking read() will receive a connection
     * closed exception.
     */
    void close();

    /**
     * Acks a message previously delivered by the consumer.
     *
     * @param deliveryToken
     *            String representing the identifier the message to be acked.
     * @throws IOException
     *             On I/O errors.
     */
    void ackDelivery(String deliveryToken) throws IOException;

    /**
     * NACKs a message previously delivered by the consumer.
     *
     * Nacked messages will be re-delivered by the service to another worker
     * based on the configured retry policies. If the application needs to
     * permanently drop a malformed message, it must issue a ACK instead of
     * NACK.
     *
     * @param deliveryToken
     *            String representing the identifier the message to be nacked.
     * @throws IOException
     *             On I/O errors.
     */
    void nackDelivery(String deliveryToken) throws IOException;

    /**
     * Blocks waiting for a message from cherami.
     *
     * This method blocks until either a message is received or if the consumer
     * is closed. On success, this method returns a CheramiDelivery object that
     * contains original message payload and a delivery token.The delivery token
     * uniquely identifies this message and must be subsequently used to either
     * ack or nack the message.
     *
     * @return A CheramiDelivery object containing the original payload and
     *         token.
     *
     * @throws InterruptedException
     *             If the blocking call is interrupted.
     * @throws IOException
     *             If the consumer is closed.
     */
    CheramiDelivery read() throws InterruptedException, IOException;

    /**
     * Asynchronously receives a message from cherami.
     *
     * This method returns a future that the caller can wait on to receive a
     * message. The future either returns a message from cherami or throws an
     * ExcecutionException if the underlying consumer is closed.
     *
     * There is no limit on the number of pending async futures / reads that can
     * be issued from the application. Its up to the application to enforce
     * limits on the concurrent pending async reads.
     *
     * The returned future *does not* support the cancel() operation. So, the
     * underlying library will hold onto the future ref in case the app decides
     * to just discard it and move on.
     *
     * @return Future that returns a CheramiDelivery object on success.
     *
     * @throws IOException
     *             On I/O errors.
     */
    Future<CheramiDelivery> readAsync() throws IOException;
}
