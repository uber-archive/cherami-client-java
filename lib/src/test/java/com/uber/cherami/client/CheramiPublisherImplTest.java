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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.cherami.CreateDestinationRequest;
import com.uber.cherami.DeleteDestinationRequest;
import com.uber.cherami.DestinationType;
import com.uber.cherami.HostAddress;
import com.uber.cherami.HostProtocol;
import com.uber.cherami.Protocol;
import com.uber.cherami.client.SendReceipt.ReceiptStatus;
import com.uber.cherami.client.metrics.DefaultMetricsClient;
import com.uber.cherami.client.metrics.MetricsReporter;
import com.uber.cherami.mocks.MockFrontendService;
import com.uber.cherami.mocks.MockSocketServlet;

/**
 * Unit tests for CheramiPublisherImpl
 */
public class CheramiPublisherImplTest {
    private static final Logger logger = LoggerFactory.getLogger(CheramiPublisherImplTest.class);
    private static final String DESTINATION_PATH = "/example/path";
    private static final long MAX_FUTURE_TIMEOUT_MILLIS = 1000;
    private static final long PUBLISH_TEST_TIMEOUT_MILLIS = 5000;
    private static final int MAX_SEND_BUFFER_BYTES = 100;
    private static CheramiClientImpl client;
    private static CheramiPublisherImpl publisher;
    private static MockFrontendService frontendService;
    private static Server server;

    private static HostProtocol newHostProtocol(String host, int port, Protocol protocol) {
        HostProtocol hostProtocol = new HostProtocol();
        List<HostAddress> hostAddrs = new ArrayList<HostAddress>();
        HostAddress addr = new HostAddress();
        addr.setHost(host);
        addr.setPort(port);
        hostAddrs.add(addr);
        hostProtocol.setProtocol(protocol);
        hostProtocol.setHostAddresses(hostAddrs);
        return hostProtocol;
    }

    @BeforeClass
    public static void setup() throws Exception {
        // Spin up mock websocketServer on a random port
        server = new Server(0);
        ServletContextHandler ctx = new ServletContextHandler();
        ctx.setContextPath("/");
        ctx.addServlet(MockSocketServlet.class, "/open_publisher_stream");
        server.setHandler(ctx);
        server.start();
        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();

        // Spin up mock FrontEnd
        frontendService = new MockFrontendService();
        MockFrontendService.createServer();
        List<HostProtocol> hostProtocols = new ArrayList<HostProtocol>();
        hostProtocols.add(newHostProtocol("127.0.0.1", port, Protocol.WS));
        hostProtocols.add(newHostProtocol("127.0.0.1", frontendService.getPort(), Protocol.TCHANNEL));
        MockFrontendService.setHosts(hostProtocols, new ArrayList<HostProtocol>());

        client = new CheramiClient.Builder("127.0.0.1", frontendService.getPort()).build();

        CreateDestinationRequest request = new CreateDestinationRequest();
        final String email = "foo@bar.com";
        final int consumedRetention = new Random().nextInt();
        final int unconsumedRetention = new Random().nextInt();
        request.setPath(DESTINATION_PATH);
        request.setOwnerEmail(email);
        request.setType(DestinationType.PLAIN);
        request.setConsumedMessagesRetention(consumedRetention);
        request.setUnconsumedMessagesRetention(unconsumedRetention);
        client.createDestination(request);
    }

    @Before
    public void newPublisher() {
        long timeout = 30 * 1000L;
        PublisherOptions options = new PublisherOptions(MAX_SEND_BUFFER_BYTES, 2, timeout);
        publisher = new CheramiPublisherImpl(client, DESTINATION_PATH, options,
                new MetricsReporter(new DefaultMetricsClient()));
    }

    @AfterClass
    public static void cleanup() throws Exception {
        DeleteDestinationRequest deleteRequest = new DeleteDestinationRequest();
        deleteRequest.setPath(DESTINATION_PATH);
        client.deleteDestination(deleteRequest);

        if (publisher.isOpen()) {
            publisher.close();
        }
        client.close();
        server.stop();
        frontendService.close();
    }

    @Test
    public void openTest() throws Exception {
        publisher.open();
        assert (publisher.isOpen() == true);
        publisher.close();
        logger.info("CheramiPublisherImplTest: PASSED: OpenTest");
    }

    @Test
    public void closeTest() throws Exception {
        if (!publisher.isOpen()) {
            publisher.open();
        }
        assert (publisher.isOpen() == true);
        publisher.close();
        assert (publisher.isOpen() == false);
        logger.info("CheramiPublisherImplTest: PASSED: CloseTest");
    }

    @Test
    public void publisherMessageTest() {
        byte[] data = "hello".getBytes();
        PublisherMessage msg = new PublisherMessage(data);
        Assert.assertEquals(0, msg.getDelaySeconds());
        Assert.assertArrayEquals(data, msg.getData());
        msg = new PublisherMessage(data, Integer.MAX_VALUE);
        Assert.assertEquals(Integer.MAX_VALUE, msg.getDelaySeconds());
        Assert.assertEquals(data, msg.getData());
    }

    @Test(timeout = PUBLISH_TEST_TIMEOUT_MILLIS)
    public void publishTest() throws Exception {
        if (!publisher.isOpen()) {
            publisher.open();
        }
        assert (publisher.isOpen() == true);

        PublisherMessage payload = new PublisherMessage("Client Hello".getBytes(UTF_8));
        SendReceipt receipt = publisher.write(payload);
        assert (receipt.getSendError() == null);
        assert (receipt.getReceipt().equals("/foo"));

        payload = new PublisherMessage("Hello, world".getBytes(UTF_8));
        receipt = publisher.write(payload);
        assert (receipt.getSendError() != null);
        publisher.close();
        logger.info("CheramiPublisherImplTest: PASSED: PublishTest");
    }

    @Test(timeout = PUBLISH_TEST_TIMEOUT_MILLIS)
    public void publishAsyncTest() throws Exception {
        if (!publisher.isOpen()) {
            publisher.open();
        }
        assert (publisher.isOpen() == true);
        PublisherMessage payload = new PublisherMessage("Client Hello".getBytes(UTF_8));
        CheramiFuture<SendReceipt> future = publisher.writeAsync(payload);
        SendReceipt receipt = future.get(MAX_FUTURE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        assert (receipt.getReceipt().equals("/foo"));
        assert (receipt.getSendError() == null);

        byte[] data = new byte[MAX_SEND_BUFFER_BYTES];
        Random rand = new Random();
        rand.nextBytes(data);
        PublisherMessage message = new PublisherMessage(data);
        // Fill up inFlight and sendBuffer
        publisher.writeAsync(message);
        publisher.writeAsync(message);
        publisher.writeAsync(message);

        CheramiFuture<SendReceipt> cheramiFuture = publisher.writeAsync(message);
        receipt = cheramiFuture.get(MAX_FUTURE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        assert (receipt.getSendError() != null);
        assert (receipt.getStatus() == ReceiptStatus.ERR_SND_BUF_FULL);

        publisher.close();
        logger.info("CheramiPublisherImplTest: PASSED: PublishAsyncTest");
    }
}
