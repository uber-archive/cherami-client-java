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

import com.uber.cherami.PutMessage;
import com.uber.cherami.client.metrics.DefaultMetricsClient;
import com.uber.cherami.client.metrics.MetricsClient;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for generating and verifying ClientOptions
 */
public class ClientOptionsTest {
    private final Logger logger = LoggerFactory.getLogger(ClientOptionsTest.class);
    private final PutMessage testPutMessage;
    private final ChecksumValidator checksumValidator;
    private ChecksumWriter checksumWriter;
    private CheramiDeliveryImpl testDelivery;

    public ClientOptionsTest() {
        this.testPutMessage = new PutMessage();
        this.testPutMessage.setData("Hello, world!".getBytes());
        this.checksumValidator = new ChecksumValidator();
    }
    
    @Test
    public void testClientOptions() {
        ClientOptions options = new ClientOptions.Builder()
                .setClientAppName("client1")
                .setDeploymentStr("staging")
                .setRpcTimeout(123)
                .setMetricsClient(null)
                .build();
        Assert.assertEquals("client1", options.getClientAppName());
        Assert.assertEquals("staging", options.getDeploymentStr());
        Assert.assertEquals(123L, options.getRpcTimeoutMillis());
        Assert.assertEquals(null, options.getMetricsClient());

        MetricsClient metricsClient = new DefaultMetricsClient();
        options = options.cloneWithMetricsClient(metricsClient);
        Assert.assertEquals("client1", options.getClientAppName());
        Assert.assertEquals("staging", options.getDeploymentStr());
        Assert.assertEquals(123L, options.getRpcTimeoutMillis());
        Assert.assertEquals(metricsClient, options.getMetricsClient());
    }

}
