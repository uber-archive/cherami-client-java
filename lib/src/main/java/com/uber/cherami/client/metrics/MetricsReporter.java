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
package com.uber.cherami.client.metrics;

/**
 * Utility to report application level metrics to a metrics system of choice.
 *
 * This implementation is a wrapper around MetricsClient that takes a
 * application level metric, converts into a low-level metric name and uses the
 * metric client to emit the metric to a metric server.
 *
 * @author venkat
 */
public class MetricsReporter {

    private final MetricsClient metricsClient;

    /**
     * Constructs and returns a MetricsReporter.
     *
     * @param metricsClient
     *            MetricsClient to be used to emit metrics.
     */
    public MetricsReporter(MetricsClient metricsClient) {
        this.metricsClient = metricsClient;
    }

    /**
     * Reports a counter value.
     *
     * @param counter
     *            Counter representing the metric name.
     * @param value
     *            Value for the counter.
     */
    public void report(Counter counter, long value) {
        this.metricsClient.reportCounter(counter.getID(), value);
    }

    /**
     * Reports a gauge value.
     *
     * @param gauge
     *            Gauge representing the metric name.
     * @param value
     *            Value for the gauge.
     */
    public void report(Gauge gauge, long value) {
        this.metricsClient.reportGauge(gauge.getID(), value);
    }

    /**
     * Reports a latency value.
     *
     * @param latency
     *            Latency representing the metric name.
     * @param elapsedNanos
     *            Value for the latency.
     */
    public void report(Latency latency, long elapsedNanos) {
        this.metricsClient.reportTimer(latency.getID(), elapsedNanos);
    }

    /**
     * Starts a latency profiler the given latency metric.
     *
     * This call must be followed by a start() and end() to profile and emit the
     * latency metric.
     *
     * @param latencyMetric
     *            Latency representing the metric name.
     * @return LatencyProfiler object.
     */
    public LatencyProfiler createLatencyProfiler(Latency latencyMetric) {
        LatencyProfiler profiler = new LatencyProfiler(this, latencyMetric);
        return profiler;
    }

    /**
     * Counter is the enum type that defines the counter names.
     *
     * @author venkat
     */
    public enum Counter {
        /** Number of publish message attempts */
        PUBLISHER_MESSAGES_OUT("cherami.publish.message.rate"),
        /** Number of publish message attempts that failed or timed out */
        PUBLISHER_MESSAGES_OUT_FAILED("cherami.publish.message.failed"),
        /** Number of acks received by the publisher from inputhosts. */
        PUBLISHER_ACKS_IN("cherami.publish.ack.rate"),
        /** Number of reconfigure commands received by the publisher. */
        PUBLISHER_RECONFIGS("cherami.publish.reconfigure.rate"),
        /** Number of messages received by the consumer. */
        CONSUMER_MESSAGES_IN("cherami.consume.message.rate"),
        /** Number of consume message attempts that failed or timed out */
        CONSUMER_MESSAGES_IN_FAILED("cherami.consumer.message.failed"),
        /** Number of credits sent from the consumer to outputhosts. */
        CONSUMER_CREDITS_OUT("cherami.consume.credit.rate"),
        /** Number of credits that failed on send or timed out. */
        CONSUMER_CREDITS_OUT_FAILED("cherami.consume.credit.failed"),
        /** Number of acks sent (attempts) by consumer to outputhosts. */
        CONSUMER_ACKS_OUT("cherami.consume.ack.rate"),
        /** Number of nacks sent (attempts) by consumer to outputhosts. */
        CONSUMER_NACKS_OUT("cherami.consume.nack.rate"),
        /** Number of acks/nacks that failed on send or timed out. */
        CONSUMER_ACKS_OUT_FAILED("cherami.consume.ack.failed"),
        /** Number of reconfigure commands received by consumer. */
        CONSUMER_RECONFIGS("cherami.consume.reconfigure.rate");

        private String id;

        Counter(String id) {
            this.id = id;
        }

        /**
         * @return Returns the underlying name of the metric.
         */
        public String getID() {
            return this.id;
        }
    }

    /**
     * Enum type for the gauge metrics.
     *
     * @author venkat
     */
    public enum Gauge {
        /** Number of connections to inputhost. */
        PUBLSIHER_NUM_CONNS("cherami.publish.connections"),
        /** Number of connections to outputhosts. */
        CONSUMER_NUM_CONNS("cherami.consume.connections"),
        /** Number of credits held locally by the consumer. */
        CONSUMER_LOCAL_CREDITS("cherami.consume.credit.local");

        private String id;

        Gauge(String id) {
            this.id = id;
        }

        /**
         * @return Returns the underlying name for the metric.
         */
        public String getID() {
            return this.id;
        }
    }

    /**
     * Enum type for the latency metrics.
     *
     * @author venkat
     */
    public enum Latency {
        /** Publish message latency. */
        PUBLISH_MESSAGE("cherami.publish.message.latency"),
        /** Latency for sendCredits. */
        SEND_CREDITS("cherami.consume.credit.latency");

        private String id;

        Latency(String id) {
            this.id = id;
        }

        /**
         * @return Returns the underlying name for the metric.
         */
        public String getID() {
            return this.id;
        }
    }

    /**
     * Utility for recording and reporting a latency metric.
     *
     * @author venkat
     */
    public static class LatencyProfiler {

        private final Latency latencyMetric;
        private final MetricsReporter reporter;

        private long startNanos;

        /**
         * Creates and returns a LatencyProfiler object.
         *
         * @param reporter
         *            MetricsReporter to be used for reporting the metric.
         * @param latencyMetric
         *            Latency metric name.
         */
        public LatencyProfiler(MetricsReporter reporter, Latency latencyMetric) {
            this.reporter = reporter;
            this.latencyMetric = latencyMetric;
        }

        /**
         * Starts the profiler.
         */
        public void start() {
            this.startNanos = System.nanoTime();
        }

        /**
         * Reports elapsed time since start(). Can be called multiple times.
         */
        public void end() {
            long elapsed = Math.min(0, System.nanoTime() - this.startNanos);
            this.reporter.report(this.latencyMetric, elapsed);
        }

        /**
         * Resets the profiler for re-use.
         */
        public void reset() {
            this.startNanos = 0;
        }
    }
}
