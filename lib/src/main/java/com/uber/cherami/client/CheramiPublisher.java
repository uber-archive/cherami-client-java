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
 * Publisher that can send messages to cherami.
 */
public interface CheramiPublisher {
    /**
     * Initiates opening of underlying streams for publishing data.
     *
     * This method initiates creation of streams / connections to cherami
     * servers for publishing data. If the destination is deleted or no longer
     * exists, this method will throw an exception. This method *does not* throw
     * any exception on intermittent i/o or discovery errors. It automatically
     * runs the backoff / retry/ reconfig protocols in the background for
     * recovering from intermittent errors.
     *
     * @throws InterruptedException
     *             If the blocking call is interrupted.
     * @throws IOException
     *             Only on unrecoverable errors.
     */
    void open() throws InterruptedException, IOException;

    /**
     * Closes all the underlying publisher streams.
     *
     * All buffered messages will be dropped and publish error will be returned
     * to the caller.
     */
    void close();

    /**
     * Publishes a message synchronously.
     *
     * This method will only return if the message is delivered / times out or
     * if the publishing fails due to some un-recoverable error. Regardless of
     * the success or failure, this method will return a SendReceipt object to
     * the caller. Callers MUST call receipt.getStatus() to find out if the
     * publishing succeeded or not. If the status returned is ERR_THROTTLED or
     * ERR_TIMED_OUT, callers must back off and retry.
     *
     * @param message
     *            PubliserMessage containing the payload to be sent.
     *
     * @return SendReceipt, message was successfully published only if
     *         receipt.getStatus is OK. Otherwise, callers must retry publishing
     *         the same message.receipt.getSendError() will return a human
     *         readable error message.
     *
     * @throws IOException
     *             On un-recoverable errors or when publisher is closed.
     * @throws InterruptedException
     *             If the blocking call is interrupted.
     *
     * @see SendReceipt for the return status codes.
     */
    SendReceipt write(PublisherMessage message) throws InterruptedException, IOException;

    /**
     * Publishes a message asynchronously.
     *
     * This method returns a future for a SendReceipt object that the caller
     * must wait on. receipt.getStatus() specifies if the publishing succeeded
     * or not.
     *
     *
     * @param message
     *            PubliserMessage containing the payload to be sent.
     *
     * @return Future that will contain the SendReceipt object once the write
     *         succeeds or fails. When the future completes, callers must check
     *         the receipt status to find out if the publishing suceeded or not.
     *         If the status is ERR_THROTTLED, callers must backoff and retry.
     *
     * @throws IOException
     *             On un-recoverable errors.
     *
     * @see SendReceipt for the return status codes.
     */
    Future<SendReceipt> writeAsync(PublisherMessage message) throws IOException;

}
