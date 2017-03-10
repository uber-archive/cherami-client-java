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

import java.util.concurrent.TimeUnit;

import com.uber.cherami.client.metrics.DefaultMetricsClient;
import com.uber.cherami.client.metrics.MetricsClient;

/**
 * Options used by CheramiClient
 */
public class ClientOptions {

    private static final long DEFAULT_RPC_TIMEOUT_MILLIS = 60 * 1000;

    /**
     * The tChannel timeout in milliseconds
     */
    private final long rpcTimeoutMillis;

    /**
     * Deployment string that gets added
     * as a suffix to the cherami service
     * name. Example - if this value is
     * staging, then the cherami hyperbahn
     * endpoint would be cherami-frontendhost_staging.
     */
    private final String deploymentStr;

    /**
     * Name of the service using the cheramiClient.
     */
    private final String clientAppName;

    /**
     * Client for metrics reporting.
     */
    private final MetricsClient metricsClient;

    private ClientOptions(Builder builder) {
        this.rpcTimeoutMillis = builder.rpcTimeoutMillis;
        this.deploymentStr = builder.deploymentStr;
        this.clientAppName = builder.clientAppName;
        this.metricsClient = builder.metricsClient;
    }

    /**
     * Constructs and returns a client options object. The deployment
     * environment is set to staging, by default. Use the Builder to to change
     * this.
     *
     * @deprecated This API is deprecated in favor the the builder.
     *
     * @param timeout
     *            rpc timeout value.
     * @param timeUnit
     *            unit for the timeout value.
     */
    @Deprecated
    public ClientOptions(long timeout, TimeUnit timeUnit) {
        this(new Builder().setRpcTimeout(timeUnit.toMillis(timeout)));
    }

    /**
     * @return
     *      Returns the rpc timeout value in millis.
     */
    public long getRpcTimeoutMillis() {
        return rpcTimeoutMillis;
    }

    /**
     * @return DeploymentStr, representing the custom suffix that will be
     *         appended to the cherami hyperbahn endpoint name.
     */
    public String getDeploymentStr() {
        return this.deploymentStr;
    }

    /**
     * Returns the client application name.
     */
    public String getClientAppName() {
        return this.clientAppName;
    }

    /**
     * @return Returns the client for metrics reporting.
     */
    public MetricsClient getMetricsClient() {
        return this.metricsClient;
    }

    /**
     * Builder is the builder for ClientOptions.
     *
     * @author venkat
     */
    public static class Builder {

        private String deploymentStr = "prod";
        private String clientAppName = "unknown";
        private MetricsClient metricsClient = new DefaultMetricsClient();
        private long rpcTimeoutMillis = DEFAULT_RPC_TIMEOUT_MILLIS;

        /**
         * Sets the rpc timeout value.
         *
         * @param timeoutMillis
         *            timeout, in millis.
         */
        public Builder setRpcTimeout(long timeoutMillis) {
            this.rpcTimeoutMillis = timeoutMillis;
            return this;
        }

        /**
         * Sets the deploymentStr.
         *
         * @param deploymentStr
         *            String representing the deployment suffix.
         */
        public Builder setDeploymentStr(String deploymentStr) {
            this.deploymentStr = deploymentStr;
            return this;
        }

        /**
         * Sets the client application name.
         *
         * This name will be used as the tchannel client service name. It will
         * also be reported as a tag along with metrics emitted to m3.
         *
         * @param clientAppName
         *            String representing the client application name.
         * @return Builder for CheramiClient.
         */
        public Builder setClientAppName(String clientAppName) {
            this.clientAppName = clientAppName;
            return this;
        }

        /**
         * Sets the metrics client to be used for metrics reporting.
         *
         * Applications must typically pass an M3 or statsd client here. By
         * default, the builder uses M3.
         *
         * @param client
         * @return
         */
        public Builder setMetricsClient(MetricsClient client) {
            this.metricsClient = client;
            return this;
        }

        /**
         * Builds and returns a ClientOptions object.
         *
         * @return ClientOptions object with the specified params.
         */
        public ClientOptions build() {
            return new ClientOptions(this);
        }
    }
}
