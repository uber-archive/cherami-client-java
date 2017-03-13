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

/**
 * Value type that keeps track of in-memory metrics.
 *
 * @author venkat
 */
public class Stats {
    /** Total number of messages published. */
    public AtomicLong messagesOutCount = new AtomicLong();

    /** Total number of bytes published. */
    public AtomicLong bytesOutCount = new AtomicLong();

    /** Total number of messages consumed. */
    public AtomicLong messagesInCount = new AtomicLong();

    /** Total number of duplicate messages consumed. */
    public AtomicLong messagesInDupCount = new AtomicLong();

    /** Total number of bytes consumed. */
    public AtomicLong bytesInCount = new AtomicLong();

    /** Total number of publish errors. */
    public AtomicLong messagesOutErrCount = new AtomicLong();

    /** Total number of messages throttled by server. */
    public AtomicLong messagesOutThrottledCount = new AtomicLong();

    /** Time taken to publish a message. */
    public final Latency writeLatency = new Latency();

    /** Time taken to consume a message. */
    public final Latency readLatency = new Latency();

    /**
     * Prints out the metric values to stdout.
     */
    public void print(long totalReportedTimeMillis) {
        System.out.println("\n-------METRICS------------");
        System.out.println("messagesOut:              " + messagesOutCount.get());
        System.out.println("bytesOut:                 " + bytesOutCount.get());
        System.out.println("messagesOutThrottled:     " + messagesOutThrottledCount.get());
        System.out.println("messagesOutErr:           " + messagesOutErrCount.get());
        System.out.println("messagesIn:               " + messagesInCount.get());
        System.out.println("messagesInDups:           " + messagesInDupCount.get());
        System.out.println("bytesIn:                  " + bytesInCount.get());
        System.out.println("publisherMsgThroughput:   " + writeMsgThroughput(totalReportedTimeMillis) + " msgs/sec");
        System.out.println("publisherBytesThroughput: " + writeBytesThroughput(totalReportedTimeMillis) + " bytes/sec");
        System.out.println("consumerMsgThroughput:    " + readMsgThroughput(totalReportedTimeMillis) + " msgs/sec");
        System.out.println("consumerBytesThroughput:  " + readBytesThroughput(totalReportedTimeMillis) + " bytes/sec");
        System.out.println("publishLatency (ms):      " + writeLatency);
        System.out.println("consumeLatency (ms):      " + readLatency);
    }

    private long writeBytesThroughput(long totalReportedTimeMillis) {
        return (bytesOutCount.get() / totalReportedTimeMillis) * 1000;
    }

    private long writeMsgThroughput(long totalReportedTimeMillis) {
        return (messagesOutCount.get() / totalReportedTimeMillis) * 1000;
    }

    private long readBytesThroughput(long totalReportedTimeMillis) {
        return (bytesInCount.get() / totalReportedTimeMillis) * 1000;
    }

    private long readMsgThroughput(long totalReportedTimeMillis) {
        return (messagesInCount.get() / totalReportedTimeMillis) * 1000;
    }

    /**
     * Represents a latency profile.
     *
     * @author venkat
     */
    public static class Latency {

        private long count;
        private long max;
        private long sum;
        private long min = Long.MAX_VALUE;

        /**
         * adds a sample to the profile.
         *
         * @param value
         *            long representing the latency
         */
        public synchronized void add(long value) {
            count++;
            sum += value;
            if (value < min) {
                min = value;
            } else if (value > max) {
                max = value;
            }
        }

        /**
         * @return Returns the average of all the samples
         */
        public long avg() {
            return (count == 0) ? 0 : (sum / count);
        }

        /**
         * @return Returns the minimum of all the sample values.
         */
        public long min() {
            return (min == Long.MAX_VALUE) ? 0 : min;
        }

        @Override
        public String toString() {
            return "sum:" + sum + ",avg:" + avg() + ",min:" + min() + ",max:" + max;
        }
    }

    /**
     * Measures elapsed time on an operation.
     *
     * @author venkat
     */
    public static class Profiler {
        private long startTimeMillis;

        /**
         * Starts the profiler, resets if previously started.
         */
        public void start() {
            this.startTimeMillis = System.currentTimeMillis();
        }

        /**
         * @return Returns the elapsed time in millis from start.
         */
        public long elapsed() {
            return System.currentTimeMillis() - startTimeMillis;
        }
    }
}

