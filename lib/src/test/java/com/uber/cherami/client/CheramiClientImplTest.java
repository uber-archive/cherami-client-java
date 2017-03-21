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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.cherami.BadRequestError;
import com.uber.cherami.ConsumerGroupDescription;
import com.uber.cherami.ConsumerGroupStatus;
import com.uber.cherami.CreateConsumerGroupRequest;
import com.uber.cherami.CreateDestinationRequest;
import com.uber.cherami.DeleteConsumerGroupRequest;
import com.uber.cherami.DeleteDestinationRequest;
import com.uber.cherami.DestinationDescription;
import com.uber.cherami.DestinationStatus;
import com.uber.cherami.DestinationType;
import com.uber.cherami.EntityAlreadyExistsError;
import com.uber.cherami.EntityNotExistsError;
import com.uber.cherami.ListConsumerGroupRequest;
import com.uber.cherami.ListDestinationsRequest;
import com.uber.cherami.ReadConsumerGroupRequest;
import com.uber.cherami.ReadDestinationRequest;
import com.uber.cherami.UpdateConsumerGroupRequest;
import com.uber.cherami.UpdateDestinationRequest;
import com.uber.cherami.client.metrics.DefaultMetricsClient;
import com.uber.cherami.client.metrics.MetricsClient;
import com.uber.cherami.mocks.MockFrontendService;

/**
 * Unit tests for CheramiClientImpl
 */
public class CheramiClientImplTest {

    private static CheramiClientImpl client;
    private static MockFrontendService frontendService;
    private static final Logger logger = LoggerFactory.getLogger(CheramiClientImpl.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * Setup the server and client channels, and initialize all the maps before executing any tests
     */
    @BeforeClass
    public static void setup() {
        frontendService = new MockFrontendService();
        try {
            MockFrontendService.createServer();
            client = new CheramiClient.Builder("127.0.0.1", frontendService.getPort()).build();
        } catch (Exception e) {
            logger.error("Error: " + e);
        }
    }

    /**
     * Shutdown the server and client channels after testing
     */
    @AfterClass
    public static void stop() {
        try {
            client.close();
            frontendService.close();
        } catch (IOException e) {
            logger.error("Error closing CheramiClient", e);
        }
    }

    /**
     * Before each test method, clear the HashMaps and reset the expected exception to none.
     * This ensures that each test stands alone and isn't affected by other tests
     */
    @Before
    public void tearDown() {
        frontendService.clearMaps();
    }

    @Test
    public void testBuilder() throws IOException {
        CheramiClient.Builder builder = new CheramiClient.Builder();
        Assert.assertTrue(builder.getHost().isEmpty());
        Assert.assertEquals(0, builder.getPort());
        Assert.assertEquals("/etc/uber/hyperbahn/hosts.json", builder.getRouterFile());
        builder.setRouterFile("/foo/bar");
        Assert.assertEquals("/foo/bar", builder.getRouterFile());
        builder = new CheramiClient.Builder("foobar.host", 9999);
        Assert.assertEquals("foobar.host", builder.getHost());
        Assert.assertEquals(9999, builder.getPort());
    }

    @Test(expected = UnknownHostException.class)
    public void testBuilderThrowsUnknownHostException() throws IOException {
        CheramiClient.Builder builder = new CheramiClient.Builder("foobar.host.12345", 9999);
        builder.build();
    }

    @Test
    public void testClientOptionsBuilder() {
        ClientOptions options = new ClientOptions.Builder().build();
        Assert.assertEquals("unknown", options.getClientAppName());
        Assert.assertEquals("prod", options.getDeploymentStr());
        Assert.assertEquals(60 * 1000L, options.getRpcTimeoutMillis());
        Assert.assertTrue(options.getMetricsClient().getClass() == DefaultMetricsClient.class);
        ClientOptions.Builder builder = new ClientOptions.Builder();
        MetricsClient metricsClient = new DefaultMetricsClient();
        builder.setClientAppName("foobar");
        builder.setDeploymentStr("staging");
        builder.setRpcTimeout(1000);
        builder.setMetricsClient(metricsClient);
        options = builder.build();
        Assert.assertEquals("foobar", options.getClientAppName());
        Assert.assertEquals("staging", options.getDeploymentStr());
        Assert.assertEquals(1000L, options.getRpcTimeoutMillis());
        Assert.assertEquals(metricsClient, options.getMetricsClient());
    }

    @Test
    public void testCreateConsumerGroup() throws Exception {
        CreateConsumerGroupRequest request = new CreateConsumerGroupRequest();
        final String destination = "/foo/bar";
        final String consumerGroup = "/test/group";
        final int lockTimeout = new Random().nextInt();
        final int maxDelivery = new Random().nextInt();
        final String email = "foo@bar.com";
        final int skipOlder = new Random().nextInt();
        final long timestamp = new Date().getTime();
        request.setDestinationPath(destination);
        request.setConsumerGroupName(consumerGroup);
        request.setLockTimeoutInSeconds(lockTimeout);
        request.setMaxDeliveryCount(maxDelivery);
        request.setOwnerEmail(email);
        request.setSkipOlderMessagesInSeconds(skipOlder);
        request.setStartFrom(timestamp);
        ConsumerGroupDescription response = client.createConsumerGroup(request);

        // Simple test case of creating a ConsumerGroup with path and group name
        assert (response.getDestinationPath().equals(destination));
        assert (response.getConsumerGroupName().equals(consumerGroup));

        // Client should not be able to create a duplicate ConsumerGroup
        thrown.expect(EntityAlreadyExistsError.class);
        client.createConsumerGroup(request);
    }

    @Test
    public void testCreateDestination() throws Exception {
        CreateDestinationRequest request = new CreateDestinationRequest();
        final String destination = "/foo/bar";
        final String email = "foo@bar.com";
        final int consumedRetention = new Random().nextInt();
        final int unconsumedRetention = new Random().nextInt();
        request.setPath(destination);
        request.setOwnerEmail(email);
        request.setType(DestinationType.PLAIN);
        request.setConsumedMessagesRetention(consumedRetention);
        request.setUnconsumedMessagesRetention(unconsumedRetention);
        DestinationDescription response = client.createDestination(request);

        // Simple test case of creating a Destination with a path and owner email
        assert (response.getPath().equals(destination));
        assert (response.getOwnerEmail().equals(email));

        // Client should not be able to create a duplicate Destination
        thrown.expect(EntityAlreadyExistsError.class);
        client.createDestination(request);
    }

    @Test
    public void testDeleteConsumerGroup() throws Exception {
        //Create a ConsumerGroup to delete
        CreateConsumerGroupRequest request = new CreateConsumerGroupRequest();
        final String destination = "/foo/bar";
        final String consumerGroup = "TestConsumerGroup";
        final int lockTimeout = new Random().nextInt();
        final int maxDelivery = new Random().nextInt();
        final String email = "foo@bar.com";
        final int skipOlder = new Random().nextInt();
        final long timestamp = new Date().getTime();
        request.setDestinationPath(destination);
        request.setConsumerGroupName(consumerGroup);
        request.setLockTimeoutInSeconds(lockTimeout);
        request.setMaxDeliveryCount(maxDelivery);
        request.setOwnerEmail(email);
        request.setSkipOlderMessagesInSeconds(skipOlder);
        request.setStartFrom(timestamp);

        ConsumerGroupDescription response = client.createConsumerGroup(request);

        assert (response.getDestinationPath().equals(destination));
        assert (response.getConsumerGroupName().equals(consumerGroup));

        DeleteConsumerGroupRequest deleteRequest = new DeleteConsumerGroupRequest();

        deleteRequest.setDestinationPath(destination);
        deleteRequest.setConsumerGroupName(consumerGroup);
        // Simple test case where we delete an existing ConsumerGroup
        try {
            client.deleteConsumerGroup(deleteRequest);
        } catch (RuntimeException e) {
            assert (false);
        }

        // Client should not be able to delete a nonexistent ConsumerGroup
        thrown.expect(EntityNotExistsError.class);
        client.deleteConsumerGroup(deleteRequest);
    }

    @Test
    public void testDeleteDestination() throws Exception {
        //Create a Destination to delete
        CreateDestinationRequest request = new CreateDestinationRequest();
        final String destination = "/foo/bar";
        final String email = "foo@bar.com";
        final int consumedRetention = new Random().nextInt();
        final int unconsumedRetention = new Random().nextInt();
        request.setPath(destination);
        request.setOwnerEmail(email);
        request.setType(DestinationType.PLAIN);
        request.setConsumedMessagesRetention(consumedRetention);
        request.setUnconsumedMessagesRetention(unconsumedRetention);

        DestinationDescription response = client.createDestination(request);

        assert (response.getPath().equals(destination));
        assert (response.getOwnerEmail().equals(email));

        DeleteDestinationRequest deleteRequest = new DeleteDestinationRequest();
        deleteRequest.setPath(destination);
        //Simple test case where we delete an existing Destination
        try {
            client.deleteDestination(deleteRequest);
        } catch (RuntimeException e) {
            assert (false);
        }
        //Client should not be able to delete a nonexistent Destination
        thrown.expect(EntityNotExistsError.class);
        client.deleteDestination(deleteRequest);
    }

    @Test
    public void testReadConsumerGroup() throws Exception {
        // Create a ConsumerGroup to read
        CreateConsumerGroupRequest request = new CreateConsumerGroupRequest();
        final String destination = "/foo/bar";
        final String consumerGroup = "TestConsumerGroup";
        final int lockTimeout = new Random().nextInt();
        final int maxDelivery = new Random().nextInt();
        final String email = "foo@bar.com";
        final int skipOlder = new Random().nextInt();
        final long timestamp = new Date().getTime();
        request.setDestinationPath(destination);
        request.setConsumerGroupName(consumerGroup);
        request.setLockTimeoutInSeconds(lockTimeout);
        request.setMaxDeliveryCount(maxDelivery);
        request.setOwnerEmail(email);
        request.setSkipOlderMessagesInSeconds(skipOlder);
        request.setStartFrom(timestamp);

        ConsumerGroupDescription response = client.createConsumerGroup(request);

        assert (response.getDestinationPath().equals(destination));
        assert (response.getConsumerGroupName().equals(consumerGroup));

        ReadConsumerGroupRequest readRequest = new ReadConsumerGroupRequest();
        readRequest.setConsumerGroupName(consumerGroup);
        readRequest.setDestinationPath(destination);

        ConsumerGroupDescription result = client.readConsumerGroup(readRequest);
        //Simple test case of reading an existing ConsumerGroup
        assert (result.getDestinationPath().equals(destination));
        assert (result.getConsumerGroupName().equals(consumerGroup));

        // Client should not be able to read from a nonexistent group.
        thrown.expect(EntityNotExistsError.class);
        readRequest.setConsumerGroupName("InvalidGroup");
        client.readConsumerGroup(readRequest);
    }

    @Test
    public void testReadDestination() throws Exception {
        // Create a Destination to read
        CreateDestinationRequest request = new CreateDestinationRequest();
        final String destination = "/foo/bar";
        final String email = "foo@bar.com";
        final int consumedRetention = new Random().nextInt();
        final int unconsumedRetention = new Random().nextInt();
        request.setPath(destination);
        request.setOwnerEmail(email);
        request.setType(DestinationType.PLAIN);
        request.setConsumedMessagesRetention(consumedRetention);
        request.setUnconsumedMessagesRetention(unconsumedRetention);

        DestinationDescription response = client.createDestination(request);

        assert (response.getPath().equals(destination));
        assert (response.getOwnerEmail().equals(email));

        ReadDestinationRequest readRequest = new ReadDestinationRequest();
        readRequest.setPath(destination);

        DestinationDescription result = client.readDestination(readRequest);
        //Simple test case of reading an existing Destination
        assert (result.getPath().equals(destination));

        //Client should not be able to read from a nonexistent destination
        thrown.expect(EntityNotExistsError.class);
        readRequest.setPath("/invalid/path");
        client.readDestination(readRequest);
    }

    @Test
    public void testUpdateConsumerGroup() throws Exception {
        //Create a ConsumerGroup to update
        CreateConsumerGroupRequest request = new CreateConsumerGroupRequest();
        final String destination = "/foo/bar";
        final String consumerGroup = "TestConsumerGroup";
        final int lockTimeout = new Random().nextInt();
        final int maxDelivery = new Random().nextInt();
        final String email = "foo@bar.com";
        final int skipOlder = new Random().nextInt();
        final long timestamp = new Date().getTime();
        request.setDestinationPath(destination);
        request.setConsumerGroupName(consumerGroup);
        request.setLockTimeoutInSeconds(lockTimeout);
        request.setMaxDeliveryCount(maxDelivery);
        request.setOwnerEmail(email);
        request.setSkipOlderMessagesInSeconds(skipOlder);
        request.setStartFrom(timestamp);

        client.createConsumerGroup(request);

        //Update the path of a group with name consumerGroup
        UpdateConsumerGroupRequest updateRequest = new UpdateConsumerGroupRequest();
        final String newDest = "/new/path/";
        updateRequest.setConsumerGroupName(consumerGroup);
        updateRequest.setDestinationPath(newDest);
        updateRequest.setOwnerEmail(email);
        updateRequest.setMaxDeliveryCount(maxDelivery);
        updateRequest.setLockTimeoutInSeconds(lockTimeout);
        updateRequest.setSkipOlderMessagesInSeconds(skipOlder);
        updateRequest.setStatus(ConsumerGroupStatus.ENABLED);

        ConsumerGroupDescription response = client.updateConsumerGroup(updateRequest);
        //Simple test case of updating an existing ConsumerGroup
        assert (response.getDestinationPath().equals(newDest));
        assert (response.getConsumerGroupName().equals(consumerGroup));

        ReadConsumerGroupRequest readRequest = new ReadConsumerGroupRequest();
        readRequest.setConsumerGroupName(consumerGroup);
        readRequest.setDestinationPath(newDest);

        ConsumerGroupDescription result = client.readConsumerGroup(readRequest);
        //Read the consumer group to make sure server actually updated
        assert (result.getDestinationPath().equals(newDest));
        assert (result.getConsumerGroupName().equals(consumerGroup));

        //Client should not be able to update a nonexistent ConsumerGroup
        thrown.expect(EntityNotExistsError.class);
        updateRequest.setConsumerGroupName("invalidGroup");
        client.updateConsumerGroup(updateRequest);
    }

    @Test
    public void testUpdateDestination() throws Exception {
        //Create Destination to update
        CreateDestinationRequest request = new CreateDestinationRequest();
        final String destination = "/foo/bar";
        final String email = "foo@bar.com";
        final int consumedRetention = new Random().nextInt();
        final int unconsumedRetention = new Random().nextInt();
        request.setPath(destination);
        request.setOwnerEmail(email);
        request.setType(DestinationType.PLAIN);
        request.setConsumedMessagesRetention(consumedRetention);
        request.setUnconsumedMessagesRetention(unconsumedRetention);

        client.createDestination(request);

        //Update the email of a Destination with path destination
        UpdateDestinationRequest updateRequest = new UpdateDestinationRequest();
        final String newEmail = "foo@uber.com";
        updateRequest.setPath(destination);
        updateRequest.setOwnerEmail(newEmail);
        updateRequest.setUnconsumedMessagesRetention(unconsumedRetention);
        updateRequest.setConsumedMessagesRetention(consumedRetention);
        updateRequest.setStatus(DestinationStatus.ENABLED);

        DestinationDescription response = client.updateDestination(updateRequest);
        // Simple case where updateDestination returns updated destination
        assert (response.getOwnerEmail().equals(newEmail));

        ReadDestinationRequest readRequest = new ReadDestinationRequest();

        readRequest.setPath(destination);

        DestinationDescription result = client.readDestination(readRequest);
        //Read Destination to make sure server actually updated
        assert (result.getPath().equals(destination));
        assert (result.getOwnerEmail().equals(newEmail));

        //Client should not be able to update a nonexistent Destination
        thrown.expect(EntityNotExistsError.class);
        updateRequest.setPath("/invalid/path");
        client.updateDestination(updateRequest);
    }

    @Test
    public void testListDestinations() throws Exception {
        CreateDestinationRequest request = new CreateDestinationRequest();
        final String destination = "/foo/bar";
        final String email = "foo@bar.com";
        final int consumedRetention = new Random().nextInt();
        final int unconsumedRetention = new Random().nextInt();
        request.setPath(destination);
        request.setOwnerEmail(email);
        request.setType(DestinationType.PLAIN);
        request.setConsumedMessagesRetention(consumedRetention);
        request.setUnconsumedMessagesRetention(unconsumedRetention);

        client.createDestination(request);

        ListDestinationsRequest listRequest = new ListDestinationsRequest();
        listRequest.setPrefix("/");
        client.listDestinations(listRequest);

        listRequest.setPrefix("/invalidPrefix");
        thrown.expect(BadRequestError.class);
        client.listDestinations(listRequest);
    }

    @Test
    public void testListConsumerGroups() throws Exception {
        CreateConsumerGroupRequest request = new CreateConsumerGroupRequest();
        final String destination = "/foo/bar";
        final String consumerGroup = "TestConsumerGroup";
        final int lockTimeout = new Random().nextInt();
        final int maxDelivery = new Random().nextInt();
        final String email = "foo@bar.com";
        final int skipOlder = new Random().nextInt();
        final long timestamp = new Date().getTime();
        request.setDestinationPath(destination);
        request.setConsumerGroupName(consumerGroup);
        request.setLockTimeoutInSeconds(lockTimeout);
        request.setMaxDeliveryCount(maxDelivery);
        request.setOwnerEmail(email);
        request.setSkipOlderMessagesInSeconds(skipOlder);
        request.setStartFrom(timestamp);

        client.createConsumerGroup(request);

        ListConsumerGroupRequest listRequest = new ListConsumerGroupRequest();
        listRequest.setDestinationPath(destination);
        listRequest.setConsumerGroupName(consumerGroup);

        //Simple test case of returning list of an existing ConsumerGroup
        client.listConsumerGroups(listRequest);

        //Client should not be able to list nonexistent ConsumerGroup
        thrown.expect(BadRequestError.class);
        listRequest.setDestinationPath("/invalid/Path");
        client.listConsumerGroups(listRequest);
    }
}