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
 * CreateConsumerRequest is passed as an argument to CheramiClient.createConsumer to create an object used by application to receive messages
 */
public class CreateConsumerRequest {

    private static final int DEFAULT_PREFETCH_COUNT = 1024;

    /**
     * Path to destination consumer wants to read messages from
     */
    private final String path;
    /**
     * ConsumerGroupName registered with Cherami for a particular destination
     */
    private final String consumerGroupName;
    /**
     * Number of messages to buffer locally.  Clients which process messages very fast may want to specify larger value
     * for PrefetchCount for faster throughput.  On the flip side larger values for PrefetchCount will result in
     * more messages being buffered locally causing high memory foot print
     */
    private final int prefetchCount;

    /**
     * CreateConsumerRequest contains options to create CheramiConsumer object
     *
     * @param path
     *            Destination path to consume messages from
     * @param groupName
     *            The consumer group name
     * @param prefetchCount
     *            The number of messages to buffer locally
     */
    private CreateConsumerRequest(String path, String groupName, int prefetchCount) {
        this.path = path;
        this.consumerGroupName = groupName;
        this.prefetchCount = prefetchCount;
    }

    public String getPath() {
        return path;
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    public int getPrefetchCount() {
        return prefetchCount;
    }

    public static class Builder {
        private final String path;
        private final String consumerGroupName;
        private int prefetchCount = DEFAULT_PREFETCH_COUNT;

        public Builder(String path, String consumerGroupName) {
            this.path = path;
            this.consumerGroupName = consumerGroupName;
        }

        public Builder setPrefetchCount(int prefetchCount) {
            this.prefetchCount = prefetchCount;
            return this;
        }

        public CreateConsumerRequest build() {
            return new CreateConsumerRequest(path, consumerGroupName, prefetchCount);
        }
    }
}
