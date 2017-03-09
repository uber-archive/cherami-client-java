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

import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.model.TestTimedOutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.cherami.CreateConsumerGroupRequest;
import com.uber.cherami.CreateDestinationRequest;
import com.uber.cherami.DeleteConsumerGroupRequest;
import com.uber.cherami.DeleteDestinationRequest;
import com.uber.cherami.DestinationType;
import com.uber.cherami.HostAddress;
import com.uber.cherami.client.metrics.DefaultMetricsClient;
import com.uber.cherami.client.metrics.MetricsReporter;
import com.uber.cherami.mocks.MockFrontendService;
import com.uber.cherami.mocks.MockOutputHostServlet;

/**
 * Unit tests for CheramiConsumerImpl
 */
public class CheramiConsumerImplTest {
    private static final Logger logger = LoggerFactory.getLogger(CheramiConsumerImplTest.class);
    private static final String DESTINATION_PATH = "/example/path";
    private static final String CONSUMER_GROUP_NAME = "test/group";
    private static final long CONSUME_TEST_TIMEOUT_MILLIS = 10000;
    private static CheramiClientImpl client;
    private static CheramiConsumerImpl consumer;
    private static MockFrontendService frontendService;
    private static Server server;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {

        // Spin up mock websocketServer on a random port
        server = new Server(0);
        ServletContextHandler ctx = new ServletContextHandler();
        ctx.setContextPath("/");
        ctx.addServlet(MockOutputHostServlet.class, "/open_consumer_stream");
        server.setHandler(ctx);
        server.start();
        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();

        HostAddress address = new HostAddress();
        address.setHost("127.0.0.1");
        address.setPort(port);
        ArrayList<HostAddress> hosts = new ArrayList<>();
        hosts.add(address);

        // Spin up mock FrontEnd
        frontendService = new MockFrontendService();
        MockFrontendService.createServer();
        MockFrontendService.setHosts(new ArrayList<HostAddress>(), hosts);

        client = new CheramiClient.Builder("127.0.0.1", frontendService.getPort()).build();

        CreateDestinationRequest request = new CreateDestinationRequest();
        final int consumedRetention = new Random().nextInt();
        final int unconsumedRetention = new Random().nextInt();
        final String email = "foo@bar.com";
        request.setPath(DESTINATION_PATH);
        request.setOwnerEmail(email);
        request.setType(DestinationType.PLAIN);
        request.setConsumedMessagesRetention(consumedRetention);
        request.setUnconsumedMessagesRetention(unconsumedRetention);
        client.createDestination(request);

        CreateConsumerGroupRequest consumerGroupRequest = new CreateConsumerGroupRequest();
        final int lockTimeout = new Random().nextInt();
        final int maxDelivery = new Random().nextInt();
        final int skipOlder = new Random().nextInt();
        final long timestamp = new Date().getTime();
        consumerGroupRequest.setDestinationPath(DESTINATION_PATH);
        consumerGroupRequest.setConsumerGroupName(CONSUMER_GROUP_NAME);
        consumerGroupRequest.setLockTimeoutInSeconds(lockTimeout);
        consumerGroupRequest.setMaxDeliveryCount(maxDelivery);
        consumerGroupRequest.setOwnerEmail(email);
        consumerGroupRequest.setSkipOlderMessagesInSeconds(skipOlder);
        consumerGroupRequest.setStartFrom(timestamp);
        client.createConsumerGroup(consumerGroupRequest);
    }

    @Before
    public void newConsumer() {
        ConsumerOptions options = new ConsumerOptions(10);
        consumer = new CheramiConsumerImpl(client, DESTINATION_PATH, CONSUMER_GROUP_NAME, options,
                new MetricsReporter(new DefaultMetricsClient()));
    }

    @AfterClass
    public static void cleanup() throws Exception {
        DeleteDestinationRequest deleteRequest = new DeleteDestinationRequest();
        deleteRequest.setPath(DESTINATION_PATH);
        client.deleteDestination(deleteRequest);

        DeleteConsumerGroupRequest deleteCGrequest = new DeleteConsumerGroupRequest();
        deleteCGrequest.setDestinationPath(DESTINATION_PATH);
        deleteCGrequest.setConsumerGroupName(CONSUMER_GROUP_NAME);

        if (consumer.isOpen()) {
            consumer.close();
        }

        client.close();
        server.stop();
        frontendService.close();
        logger.info("CheramiConsumerImplTest: ALL PASSED");
    }

    @Test
    public void openTest() throws Exception {
        consumer.open();
        assert (consumer.isOpen() == true);
        consumer.close();
        logger.info("CheramiConsumerImplTest: PASSED: OpenTest");
    }

    @Test
    public void closeTest() throws Exception {
        if (!consumer.isOpen()) {
            consumer.open();
        }
        assert (consumer.isOpen() == true);
        consumer.close();
        assert (consumer.isOpen() == false);
        logger.info("CheramiConsumerImplTest: PASSED: CloseTest");
    }

    @Test(timeout = CONSUME_TEST_TIMEOUT_MILLIS)
    public void readTest() throws Exception {
        if (!consumer.isOpen()) {
            consumer.open();
        }
        ArrayList<String> ids = new ArrayList<>(200);
        for (int i = 0; i < 200; i++) {
            ids.add(Integer.toString(i));
        }
        for (int i = 0; i < 200; i++) {
            CheramiDelivery delivery = consumer.read();
            String id = delivery.getMessage().getAckId();
            ids.remove(id);
        }
        assert (ids.isEmpty());

        //Test that read blocks if there are no messages
        thrown.expect(TestTimedOutException.class);
        consumer.read();
    }

    @Test(timeout = CONSUME_TEST_TIMEOUT_MILLIS)
    public void readAsyncTest() throws Exception {
        if (!consumer.isOpen()) {
            consumer.open();
        }
        ArrayList<String> ids = new ArrayList<>(200);
        for (int i = 0; i < 200; i++) {
            ids.add(Integer.toString(i));
        }

        ArrayList<CheramiFuture<CheramiDelivery>> futures = new ArrayList<>(200);
        for (int i = 0; i < 200; i++) {
            futures.add(consumer.readAsync());
        }

        for (int i = 0; i < 200; i++) {
            CheramiFuture<CheramiDelivery> future = futures.remove(0);
            CheramiDelivery delivery = future.get();
            String id = delivery.getMessage().getAckId();
            ids.remove(id);
        }
        assert (ids.isEmpty());

        //Test that the future is set to error.
        CheramiFuture<CheramiDelivery> future = consumer.readAsync();
        consumer.close();
        thrown.expect(ExecutionException.class);
        future.get();
        logger.info("CheramiConsumerImplTest: PASSED: ConsumeAsyncTest");
    }
}