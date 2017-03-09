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

/**
 * SendReceipt is the token that gets returned as a receipt for publishing.
 */
public class SendReceipt {
    /**
     * The local reference ID upon publishing. It is unique only with respect to a single Sender
     */
    private final String id;
    /**
     * receipt is a opaque token that is non-empty when the message is
     * successfully published.
     */
    private final String receipt;
    /**
     * Human readable error message associated with the status code.
     */
    private final Error sendError;

    /** ReceiptStatus enum representing the result of publishing. */
    private final ReceiptStatus status;

    /**
     * Creates a SendReceipt object.
     *
     * @param id
     *            String representing the message ID.
     * @param receipt
     *            String representing the token from server.
     * @param status
     *            ReceiptStatus indicating the publishing result.
     * @param error
     *            Any error associated with the receipt status.
     */
    public SendReceipt(String id, String receipt, ReceiptStatus status, Error error) {
        this.id = id;
        this.receipt = receipt;
        this.status = status;
        this.sendError = error;
    }

    /**
     * Constructs a SendReceipt object with empty receipt.
     *
     * @param id
     *            String representing the message ID.
     * @param status
     *            ReceiptStatus indicating the publishing result.
     * @param error
     *            Any error associated with the receipt status.
     */
    public SendReceipt(String id, ReceiptStatus status, Error error) {
        this(id, "", status, error);
    }

    /**
     * Creates a SendReceipt object with status OK and no error.
     *
     * @param id
     *            String representing the message ID.
     * @param receipt
     *            String representing the token from server.
     */
    public SendReceipt(String id, String receipt) {
        this(id, receipt, ReceiptStatus.OK, null);
    }

    /**
     * @return String representing the messageID that this receipt is associated
     *         with.
     */
    public String getId() {
        return id;
    }

    /**
     * @return String representing the token from server.
     */
    public String getReceipt() {
        return receipt;
    }

    /**
     * @return Error, if the publishing failed, null otherwise.
     */
    public Error getSendError() {
        return sendError;
    }

    /**
     * @return ReceiptStatus enum indicating the result of publishing.
     */
    public ReceiptStatus getStatus() {
        return status;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("{");
        buffer.append("id=" + id);
        buffer.append(", receipt=" + receipt);
        buffer.append(", status=" + status.name());
        buffer.append(", error=" + ((sendError == null) ? "null" : sendError.getMessage()));
        buffer.append("}");
        return buffer.toString();
    }

    /**
     * Enum that represents the result of publishig a message.
     *
     * @author venkat
     */
    public static enum ReceiptStatus {
        /** Successfully published. */
        OK,
        /** Client or server timeout. */
        ERR_TIMED_OUT,
        /** Message cannot be enqueued due to send buffer for processing. */
        ERR_SND_BUF_FULL,
        /** Underlying stream returned error. */
        ERR_SOCKET,
        /** No connections available for publishing message. */
        ERR_NO_CONNS,
        /** The connection used by this message was closed. */
        ERR_CONN_CLOSED,
        /** Message throttled by server. */
        ERR_THROTTLED,
        /** Any other internal service error. */
        ERR_SERVICE;
    }
}
