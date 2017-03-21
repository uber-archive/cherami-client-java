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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.servlet.Servlet;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
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
import com.uber.cherami.mocks.MockSocketServlet;
import com.uber.tchannel.api.TChannel;

/**
 * Run multiple integration test scenarios
 */
@Ignore
public class CheramiIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(CheramiIntegrationTest.class);
    private static final String DESTINATION_PATH = "/reconfigure/test";
    private static final String CONSUMER_GROUP_NAME = "test/group";
    private static final String PUBLISHER_WEBSOCKET_ENDPOINT = "/open_publisher_stream";
    private static final String CONSUMER_WEBSOCKET_ENDPOINT = "/open_consumer_stream";
    private static final long MAX_FUTURE_TIMEOUT_MILLIS = 1000;
    private static final long TEST_TIMEOUT_MILLIS = 10000;
    private static CheramiClientImpl client;
    private static CheramiPublisherImpl publisher;
    private static CheramiConsumerImpl consumer;
    private static MockFrontendService frontendService;
    private static Server inputServer;
    private static Server inputServer1;
    private static Server outputServer;
    private static Server outputServer1;
    private static ArrayList<HostAddress> inputHosts;
    private static ArrayList<HostAddress> outputHosts;
    private static IntegrationConfig config;

    private static Server createServer(Class<? extends Servlet> servlet, String endpoint, List<HostAddress> hosts) throws Exception {
        Server server = new Server(0);
        ServletContextHandler ctx = new ServletContextHandler();
        ctx.setContextPath("/");
        ctx.addServlet(servlet, endpoint);
        server.setHandler(ctx);
        server.start();
        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();

        HostAddress address = new HostAddress();
        address.setHost("127.0.0.1");
        address.setPort(port);
        hosts.add(address);

        return server;
    }

    @BeforeClass
    public static void setup() throws Exception {
        LogManager.getLogger("io.netty").setLevel(Level.WARN);
        // Create inputhost and outputhost servers
        inputHosts = new ArrayList<>();
        inputServer = createServer(MockSocketServlet.class, PUBLISHER_WEBSOCKET_ENDPOINT, inputHosts);
        inputServer1 = createServer(MockSocketServlet.class, PUBLISHER_WEBSOCKET_ENDPOINT, inputHosts);

        outputHosts = new ArrayList<>();
        outputServer = createServer(MockOutputHostServlet.class, CONSUMER_WEBSOCKET_ENDPOINT, outputHosts);
        outputServer1 = createServer(MockOutputHostServlet.class, CONSUMER_WEBSOCKET_ENDPOINT, outputHosts);

        // Spin up mock FrontEnd
        frontendService = new MockFrontendService();
        MockFrontendService.createServer();
        MockFrontendService.setHosts(inputHosts, outputHosts);

        client = new CheramiClient.Builder("127.0.0.1", frontendService.getPort()).build();
        config = new IntegrationConfig(100, 1024, 10, 500, 200);

        CreateDestinationRequest request = new CreateDestinationRequest();
        String email = "foo@bar.com";
        int consumedRetention = new Random().nextInt();
        int unconsumedRetention = new Random().nextInt();
        request.setPath(DESTINATION_PATH);
        request.setOwnerEmail(email);
        request.setType(DestinationType.PLAIN);
        request.setConsumedMessagesRetention(consumedRetention);
        request.setUnconsumedMessagesRetention(unconsumedRetention);
        client.createDestination(request);

        CreateConsumerGroupRequest cGrequest = new CreateConsumerGroupRequest();
        cGrequest.setConsumerGroupName(CONSUMER_GROUP_NAME);
        cGrequest.setDestinationPath(DESTINATION_PATH);
        cGrequest.setOwnerEmail(email);
        cGrequest.setLockTimeoutInSeconds(10);
        cGrequest.setMaxDeliveryCount(3);
        client.createConsumerGroup(cGrequest);
    }

    public void newPublisher() {
        long timeout = 30 * 1000L;
        PublisherOptions options = new PublisherOptions(config.maxSendBufferBytes, config.maxInflightMsgs, timeout);
        publisher = new CheramiPublisherImpl(client, DESTINATION_PATH, options,
                new MetricsReporter(new DefaultMetricsClient()));
    }

    public void newConsumer() {
        ConsumerOptions options = new ConsumerOptions(config.prefetchSize);
        consumer = new CheramiConsumerImpl(client, DESTINATION_PATH, CONSUMER_GROUP_NAME, options,
                new MetricsReporter(new DefaultMetricsClient()));
    }

    @AfterClass
    public static void cleanup() throws Exception {
        DeleteDestinationRequest deleteRequest = new DeleteDestinationRequest();
        deleteRequest.setPath(DESTINATION_PATH);
        client.deleteDestination(deleteRequest);

        DeleteConsumerGroupRequest deleteCGrequest = new DeleteConsumerGroupRequest();
        deleteCGrequest.setConsumerGroupName(CONSUMER_GROUP_NAME);
        deleteCGrequest.setDestinationPath(DESTINATION_PATH);
        client.deleteConsumerGroup(deleteCGrequest);

        publisher.close();
        consumer.close();
        client.close();
        inputServer.stop();
        outputServer.stop();
        frontendService.close();
    }

    @Test(timeout = TEST_TIMEOUT_MILLIS)
    public void reconfigurePublisherTest() throws Exception {
        newPublisher();
        if (!publisher.isOpen()) {
            publisher.open();
        }
        assert (publisher.isOpen() == true);

        byte[] data = "Client Hello".getBytes(UTF_8);
        Queue<CheramiFuture<SendReceipt>> futures = new ArrayDeque<>();
        PublisherMessage message = new PublisherMessage(data);


        for (int i = 0; i < config.numMessagesToPublish; i++) {
            CheramiFuture<SendReceipt> future = publisher.writeAsync(message);
            futures.add(future);
        }

        // Stop one of the servers after 100 ms
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    inputServer1.stop();
                    inputHosts.remove(1);
                    MockFrontendService.setHosts(inputHosts, outputHosts);
                } catch (Exception e) {
                    logger.error("Error in stopping server", e);
                }
            }
        }, 100);

        int numPublished = 0;
        while (numPublished < config.numMessagesToPublish) {
            CheramiFuture<SendReceipt> future = futures.poll();
            try {
                SendReceipt receipt = future.get(MAX_FUTURE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (receipt.getSendError() == null) {
                    numPublished++;
                } else {
                    CheramiFuture<SendReceipt> resendFuture = publisher.writeAsync(message);
                    futures.add(resendFuture);
                }
            } catch (TimeoutException e) {
                logger.error("Timeout error. Test failed", e);
                assert (false);
            }
        }

        timer.cancel();
    }

    @Test(timeout = TEST_TIMEOUT_MILLIS)
    public void reconfigureConsumerTest() throws Exception {
        newConsumer();

        if (!consumer.isOpen()) {
            consumer.open();
        }
        assert (consumer.isOpen() == true);

        List<TChannel> outputChannels = getOutputTChannels();

        // Stop one of the servers after 75 ms
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    outputServer1.stop();
                    outputHosts.remove(1);
                    MockFrontendService.setHosts(inputHosts, outputHosts);
                } catch (Exception e) {
                    logger.error("Error in stopping server", e);
                }
            }
        }, 75);

        ArrayList<String> receivedIds = new ArrayList<>(config.numMessagesToConsume);
        int numReceived = 0;
        while (numReceived < config.numMessagesToConsume) {
            CheramiDelivery delivery = consumer.read();
            if (!receivedIds.contains(delivery.getMessage().getAckId())) {
                receivedIds.add(delivery.getMessage().getAckId());
                delivery.ack();
                numReceived++;
            }
        }

        timer.cancel();
        consumer.close();

        for (TChannel outputChannel : outputChannels) {
            outputChannel.shutdown();
        }
    }

    private List<TChannel> getOutputTChannels() throws Exception {
        List<TChannel> outputChannels = new ArrayList<>(outputHosts.size());
        for (HostAddress outputServer : outputHosts) {
            String host = outputServer.getHost();
            int port = outputServer.getPort();
            TChannel channel = frontendService.startOutputTChannel(host, port);
            channel.listen();
            outputChannels.add(channel);
        }
        return outputChannels;
    }

    private static class IntegrationConfig {
        final int maxInflightMsgs;
        final int maxSendBufferBytes;
        final int prefetchSize;
        final int numMessagesToPublish;
        final int numMessagesToConsume;

        IntegrationConfig(int maxInflight, int maxSendBufferBytes, int prefetchSize, int numToPublish, int numToConsume) {
            this.maxInflightMsgs = maxInflight;
            this.maxSendBufferBytes = maxSendBufferBytes;
            this.prefetchSize = prefetchSize;
            this.numMessagesToPublish = numToPublish;
            this.numMessagesToConsume = numToConsume;
        }
    }

}
