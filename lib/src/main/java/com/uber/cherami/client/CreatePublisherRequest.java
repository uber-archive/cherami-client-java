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
 * CreatePublisherRequest is passed as an argument to CheramiClient.createPublisher to create an object used by application to publish messages
 */
public class CreatePublisherRequest {

    private static final int DEFAULT_INFLIGHT_MESSAGES = 1024;
    private static final int DEFAULT_BUFFER_SIZE_BYTES = 1024 * 1024;
    private static final long DEFAULT_WRITE_TIMEOUT_MILLIS = 60 * 1000;

    private final String path;
    private final int maxInflightMessagesPerConnection;
    private final long sendBufferSizeBytes;
    private final long writeTimeoutMillis;

    /**
     * CreatePublisherRequest contains options to create CheramiPublisher object
     *
     * @param path
     *            Destination path CheramiPublisher should write to
     * @param messagesPerConnection
     *            The max number of inflight messages each connection can have
     * @param bufferSizeBytes
     *            The max number of bytes that can be queued for processing.
     *            This is not the same as enqueueing to the server
     * @param writeTimeoutMillis
     *            Time after which the publisher gives up writing a message.
     */
    private CreatePublisherRequest(String path, int messagesPerConnection, long bufferSizeBytes,
            long writeTimeoutMillis) {
        this.path = path;
        this.writeTimeoutMillis = writeTimeoutMillis;
        this.maxInflightMessagesPerConnection = messagesPerConnection;
        this.sendBufferSizeBytes = bufferSizeBytes;
    }

    public String getPath() {
        return path;
    }

    public int getMaxInflightMessagesPerConnection() {
        return maxInflightMessagesPerConnection;
    }

    public long getSendBufferSizeBytes() {
        return sendBufferSizeBytes;
    }

    public long getWriteTimeoutMillis() {
        return writeTimeoutMillis;
    }

    public static class Builder {
        private final String path;
        private int messagesPerConnection = DEFAULT_INFLIGHT_MESSAGES;
        private long bufferSizeBytes = DEFAULT_BUFFER_SIZE_BYTES;
        private long writeTimeoutMillis = DEFAULT_WRITE_TIMEOUT_MILLIS;

        public Builder(String path) {
            this.path = path;
        }

        public Builder setKeepAlive(boolean keepAlive) {
            // no-op on purpose, this option shouldn't
            // be exposed, but it is. Need to work
            // with customers before removing this
            // option.
            return this;
        }

        public Builder setMessagesPerConnection(int messagesPerConnection) {
            this.messagesPerConnection = messagesPerConnection;
            return this;
        }

        public Builder setBufferSizeBytes(long bufferSizeBytes) {
            this.bufferSizeBytes = bufferSizeBytes;
            return this;
        }

        public Builder setWriteTimeoutMillis(long writeTimeoutMillis) {
            this.writeTimeoutMillis = writeTimeoutMillis;
            return this;
        }

        public CreatePublisherRequest build() {
            return new CreatePublisherRequest(path, messagesPerConnection, bufferSizeBytes,
                    writeTimeoutMillis);
        }
    }
}
