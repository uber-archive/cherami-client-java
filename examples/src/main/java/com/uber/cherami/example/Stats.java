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
package com.uber.cherami.example;

import java.util.concurrent.atomic.AtomicLong;

public class Stats {

    public AtomicLong messagesOutCount = new AtomicLong();
    public AtomicLong bytesOutCount = new AtomicLong();
    public AtomicLong messagesInCount = new AtomicLong();
    public AtomicLong messagesInDupCount = new AtomicLong();
    public AtomicLong bytesInCount = new AtomicLong();
    public AtomicLong messagesOutErrCount = new AtomicLong();
    public AtomicLong messagesOutThrottledCount = new AtomicLong();

    public final Latency writeLatency = new Latency();
    public final Latency readLatency = new Latency();
    public final Latency totalPublishLatency = new Latency();

    public void print() {
        System.out.println("\n-------METRICS------------");
        System.out.println("messagesOut:              " + messagesOutCount.get());
        System.out.println("bytesOut:                 " + bytesOutCount.get());
        System.out.println("messagesOutThrottled:     " + messagesOutThrottledCount.get());
        System.out.println("messagesOutErr:           " + messagesOutErrCount.get());
        System.out.println("messagesIn:               " + messagesInCount.get());
        System.out.println("messagesInDups:           " + messagesInDupCount.get());
        System.out.println("bytesIn:                  " + bytesInCount.get());
        System.out.println("publisherMsgThroughput:   " + writeMsgThroughput() + " msgs/sec");
        System.out.println("publisherBytesThroughput: " + writeBytesThroughput() + " bytes/sec");
        System.out.println("consumerMsgThroughput:    " + readMsgThroughput() + " msgs/sec");
        System.out.println("consumerBytesThroughput:  " + readBytesThroughput() + " bytes/sec");
        System.out.println("publishLatency (ms):      " + writeLatency);
        System.out.println("consumeLatency (ms):      " + readLatency);
    }

    private long writeBytesThroughput() {
        if (totalPublishLatency.sum == 0) {
            return 0;
        }
        return (bytesOutCount.get() / totalPublishLatency.sum) * 1000;
    }

    private long writeMsgThroughput() {
        if (totalPublishLatency.sum == 0) {
            return 0;
        }
        return (messagesOutCount.get() / totalPublishLatency.sum) * 1000;
    }

    private long readBytesThroughput() {
        if (readLatency.sum == 0) {
            return 0;
        }
        return (bytesInCount.get() / readLatency.sum) * 1000;
    }

    private long readMsgThroughput() {
        if (readLatency.sum == 0) {
            return 0;
        }
        return (messagesInCount.get() / readLatency.sum) * 1000;
    }

    public static class Latency {

        private long count;
        private long max;
        private long sum;
        private long min = Long.MAX_VALUE;

        public synchronized void add(long value) {
            count++;
            sum += value;
            if (value < min) {
                min = value;
            } else if (value > max) {
                max = value;
            }
        }

        public long avg() {
            return (count == 0) ? 0 : (sum / count);
        }

        public long min() {
            return (min == Long.MAX_VALUE) ? 0 : min;
        }

        @Override
        public String toString() {
            return "sum:" + sum + ",avg:" + avg() + ",min:" + min() + ",max:" + max;
        }
    }
}

