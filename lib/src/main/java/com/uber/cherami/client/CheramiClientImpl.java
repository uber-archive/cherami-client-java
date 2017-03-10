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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.uber.cherami.BFrontend.createConsumerGroup_args;
import com.uber.cherami.BFrontend.createConsumerGroup_result;
import com.uber.cherami.BFrontend.createDestination_args;
import com.uber.cherami.BFrontend.createDestination_result;
import com.uber.cherami.BFrontend.deleteConsumerGroup_args;
import com.uber.cherami.BFrontend.deleteConsumerGroup_result;
import com.uber.cherami.BFrontend.deleteDestination_args;
import com.uber.cherami.BFrontend.deleteDestination_result;
import com.uber.cherami.BFrontend.listConsumerGroups_args;
import com.uber.cherami.BFrontend.listConsumerGroups_result;
import com.uber.cherami.BFrontend.listDestinations_args;
import com.uber.cherami.BFrontend.listDestinations_result;
import com.uber.cherami.BFrontend.readConsumerGroupHosts_args;
import com.uber.cherami.BFrontend.readConsumerGroupHosts_result;
import com.uber.cherami.BFrontend.readConsumerGroup_args;
import com.uber.cherami.BFrontend.readConsumerGroup_result;
import com.uber.cherami.BFrontend.readDestinationHosts_args;
import com.uber.cherami.BFrontend.readDestinationHosts_result;
import com.uber.cherami.BFrontend.readDestination_args;
import com.uber.cherami.BFrontend.readDestination_result;
import com.uber.cherami.BFrontend.readPublisherOptions_args;
import com.uber.cherami.BFrontend.readPublisherOptions_result;
import com.uber.cherami.BFrontend.updateConsumerGroup_args;
import com.uber.cherami.BFrontend.updateConsumerGroup_result;
import com.uber.cherami.BFrontend.updateDestination_args;
import com.uber.cherami.BFrontend.updateDestination_result;
import com.uber.cherami.BadRequestError;
import com.uber.cherami.ConsumerGroupDescription;
import com.uber.cherami.CreateConsumerGroupRequest;
import com.uber.cherami.CreateDestinationRequest;
import com.uber.cherami.DeleteConsumerGroupRequest;
import com.uber.cherami.DeleteDestinationRequest;
import com.uber.cherami.DestinationDescription;
import com.uber.cherami.EntityAlreadyExistsError;
import com.uber.cherami.EntityDisabledError;
import com.uber.cherami.EntityNotExistsError;
import com.uber.cherami.HostAddress;
import com.uber.cherami.ListConsumerGroupRequest;
import com.uber.cherami.ListConsumerGroupResult;
import com.uber.cherami.ListDestinationsRequest;
import com.uber.cherami.ListDestinationsResult;
import com.uber.cherami.ReadConsumerGroupHostsRequest;
import com.uber.cherami.ReadConsumerGroupRequest;
import com.uber.cherami.ReadDestinationHostsRequest;
import com.uber.cherami.ReadDestinationRequest;
import com.uber.cherami.ReadPublisherOptionsRequest;
import com.uber.cherami.ReadPublisherOptionsResult;
import com.uber.cherami.UpdateConsumerGroupRequest;
import com.uber.cherami.UpdateDestinationRequest;
import com.uber.cherami.client.metrics.MetricsReporter;
import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.errors.TChannelError;
import com.uber.tchannel.hyperbahn.api.HyperbahnClient;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;

/**
 * An implementation of the CheramiClient interface
 */
public class CheramiClientImpl implements CheramiClient {
    /**
     * The Jetty WebSocketClient suggests that only one WebSocketClient should be created per JVM.
     * This is a static singleton is lazily initialized in a thread safe way when CheramiClientImpl is initialized.
     * It also gets started and stopped only once.
     */
    private static class ClientHolder {
        private static final WebSocketClient wsClient = new WebSocketClient();
    }

    private static final Logger logger = LoggerFactory.getLogger(CheramiClientImpl.class);

    private static final int DEFAULT_PAGE_SIZE = 1000;
    private static final String FRONTEND_SERVICE_NAME = "cherami-frontendhost";
    private static final String INTERFACE_NAME = "BFrontend";

    private final TChannel tChannel;
    private final ClientOptions options;
    private final String serviceName;
    private final Map<String, String> thriftHeaders;
    private final MetricsReporter metricsReporter;

    /**
     * hyperbahnClient used to build SubChanel. Null if client doesn't use Hyperbahn
     */
    private HyperbahnClient hyperbahnClient;
    /**
     * Subchannel used to actually make requests
     */
    private SubChannel subChannel;

    private CheramiClientImpl(ClientOptions options) throws IOException {
        this.options = options;
        this.serviceName = getFrontendServiceName();
        String envUserName = System.getenv("USER");
        String envHostname;
        try {
            envHostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            envHostname = "localhost";
        }
        this.thriftHeaders = ImmutableMap.<String, String>builder()
                .put("user-name", envUserName)
                .put("host-name", envHostname)
                .build();
        try {
            WebSocketClient wsClient = getWebsocketClient();
            wsClient.start();
            logger.info("CheramiClient created for service {}", serviceName);
        } catch (Exception e) {
            logger.error("Failed to start WebSocketClient", e);
            throw new IOException(e);
        }
        this.metricsReporter = new MetricsReporter(options.getMetricsClient());
        // Need to create tChannel last in order to prevent leaking when an exception is thrown
        this.tChannel = new TChannel.Builder(options.getClientAppName()).build();
    }

    /**
     * Constructor that creates a CheramiClient object by taking in a host address and port
     *
     * @param host    IP address of server
     * @param port    Port on which to connect to server
     * @param options A ClientOptions object.
     * @throws IOException
     */
    protected CheramiClientImpl(String host, int port, ClientOptions options) throws IOException {
        this(options);
        this.hyperbahnClient = null;
        try {
            InetAddress addr = InetAddress.getByName(host);
            ArrayList<InetSocketAddress> peers = new ArrayList<>();
            peers.add(new InetSocketAddress(addr, port));
            this.subChannel = tChannel.makeSubChannel(this.serviceName).setPeers(peers);
        } catch (UnknownHostException e) {
            logger.error("Unable to get name of host {}", host);
            tChannel.shutdown();
            throw e;
        }
    }

    protected CheramiClientImpl(String routerFile, ClientOptions options) throws IOException {
        this(options);
        try {
            this.hyperbahnClient = new HyperbahnClient.Builder(tChannel.getServiceName(), tChannel)
                    .setRouterFile(routerFile).build();
            this.subChannel = hyperbahnClient.makeClientChannel(this.serviceName);
        } catch (IOException e) {
            tChannel.shutdown();
            throw e;
        }
    }

    protected MetricsReporter getMetricsReporter() {
        return this.metricsReporter;
    }

    @Override
    public ClientOptions getOptions() {
        return this.options;
    }

    private boolean isProd(String deploymentStr) {
        return (deploymentStr == null || deploymentStr.isEmpty() || deploymentStr.toLowerCase().startsWith("prod"));
    }

    private String getFrontendServiceName() {
        String deploymentStr = options.getDeploymentStr();
        if (this.isProd(deploymentStr)) {
            return FRONTEND_SERVICE_NAME;
        }
        return String.format("%s_%s", FRONTEND_SERVICE_NAME, deploymentStr);
    }

    protected static WebSocketClient getWebsocketClient() {
        return ClientHolder.wsClient;
    }

    /**
     * Returns the endpoint in the format service::method"
     */
    private static String getEndpoint(String service, String method) {
        return String.format("%s::%s", service, method);
    }

    private void throwOnRpcError(ThriftResponse<?> response) throws IOException {
        if (response.isError()) {
            throw new IOException("Rpc error:" + response.getError());
        }
    }

    private void throwOnNullRequest(Object request) {
        if (request == null) {
            throw new IllegalArgumentException("request is null");
        }
    }

    private <T> ThriftRequest<T> buildThriftRequest(String apiName, T body) {
        String endpoint = getEndpoint(INTERFACE_NAME, apiName);
        ThriftRequest.Builder<T> builder = new ThriftRequest.Builder<T>(serviceName, endpoint);
        builder.setHeaders(thriftHeaders);
        builder.setTimeout(this.options.getRpcTimeoutMillis());
        builder.setBody(body);
        return builder.build();
    }

    private <T> ThriftResponse<T> doRemoteCall(ThriftRequest<?> request) throws IOException {
        ThriftResponse<T> response = null;
        try {
            TFuture<ThriftResponse<T>> future = subChannel.send(request);
            response = future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e);
        } catch (TChannelError e) {
            throw new IOException("Rpc error", e);
        }
        this.throwOnRpcError(response);
        return response;
    }

    /**
     * Shutdown the TChannel
     */
    @Override
    public void close() throws IOException {
        if (tChannel != null) {
            tChannel.shutdown();
        }
        if (hyperbahnClient != null) {
            hyperbahnClient.shutdown();
        }
        try {
            WebSocketClient wsClient = getWebsocketClient();
            wsClient.stop();
        } catch (Exception e) {
            logger.error("Error stopping WebSocket client", e);
            throw new IOException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerGroupDescription createConsumerGroup(CreateConsumerGroupRequest request)
            throws EntityAlreadyExistsError, BadRequestError, IOException {

        this.throwOnNullRequest(request);

        if (!request.isSetConsumerGroupName()) {
            throw new BadRequestError("request missing consumerGroupName parameter");
        }
        if (!request.isSetDestinationPath()) {
            throw new BadRequestError("request missing destinationPath parameter");
        }

        ThriftRequest<createConsumerGroup_args> thriftRequest = null;
        ThriftResponse<createConsumerGroup_result> thriftResponse = null;
        try {

            thriftRequest = buildThriftRequest("createConsumerGroup", new createConsumerGroup_args(request));
            thriftResponse = doRemoteCall(thriftRequest);
            createConsumerGroup_result result = thriftResponse.getBody(createConsumerGroup_result.class);
            if (thriftResponse.getResponseCode() == ResponseCode.OK) {
                return result.getSuccess();
            }
            if (result != null && result.isSetEntityExistsError()) {
                throw result.getEntityExistsError();
            }
            if (result != null && result.isSetRequestError()) {
                throw result.getRequestError();
            }
            throw new IOException("createConsumerGroup failed with unknown error:" + result);
        } finally {
            if (thriftResponse != null) {
                thriftResponse.release();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DestinationDescription createDestination(CreateDestinationRequest request)
            throws EntityAlreadyExistsError, BadRequestError, IOException {

        this.throwOnNullRequest(request);
        if (!request.isSetPath()) {
            throw new BadRequestError("request missing 'path' parameter");
        }

        ThriftRequest<createDestination_args> thriftRequest = null;
        ThriftResponse<createDestination_result> thriftResponse = null;
        try {
            thriftRequest = buildThriftRequest("createDestination", new createDestination_args(request));
            thriftResponse = doRemoteCall(thriftRequest);
            createDestination_result result = thriftResponse.getBody(createDestination_result.class);
            if (thriftResponse.getResponseCode() == ResponseCode.OK) {
                return result.getSuccess();
            }
            if (result != null && result.isSetEntityExistsError()) {
                throw result.getEntityExistsError();
            }
            if (result != null && result.isSetRequestError()) {
                throw result.getRequestError();
            }
            throw new IOException("createDestination failed with unknown error:" + result);

        } finally {
            if (thriftResponse != null) {
                thriftResponse.release();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CheramiPublisher createPublisher(CreatePublisherRequest request) {
        PublisherOptions options = new PublisherOptions(request.getSendBufferSizeBytes(),
                request.getMaxInflightMessagesPerConnection(), request.getWriteTimeoutMillis());
        return new CheramiPublisherImpl(this, request.getPath(), options, metricsReporter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CheramiConsumer createConsumer(CreateConsumerRequest request) {
        ConsumerOptions options = new ConsumerOptions(request.getPrefetchCount());
        return new CheramiConsumerImpl(this, request.getPath(), request.getConsumerGroupName(), options,
                metricsReporter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteConsumerGroup(DeleteConsumerGroupRequest request)
            throws EntityNotExistsError, BadRequestError, IOException {

        this.throwOnNullRequest(request);

        if (!request.isSetConsumerGroupName()) {
            throw new BadRequestError("request missing consumerGroupName parameter");
        }
        if (!request.isSetDestinationPath()) {
            throw new BadRequestError("request missing destinationPath parameter");
        }

        ThriftRequest<deleteConsumerGroup_args> thriftRequest = null;
        ThriftResponse<deleteConsumerGroup_result> thriftResponse = null;
        try {
            thriftRequest = buildThriftRequest("deleteConsumerGroup", new deleteConsumerGroup_args(request));
            thriftResponse = doRemoteCall(thriftRequest);
            deleteConsumerGroup_result result = thriftResponse.getBody(deleteConsumerGroup_result.class);
            if (thriftResponse.getResponseCode() == ResponseCode.OK) {
                return;
            }
            if (result != null && result.isSetEntityError()) {
                throw result.getEntityError();
            }
            if (result != null && result.isSetRequestError()) {
                throw result.getRequestError();
            }

            throw new IOException("deleteConsumerGroup failed with unknown error:" + result);
        } finally {
            if (thriftResponse != null) {
                thriftResponse.release();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDestination(DeleteDestinationRequest request)
            throws BadRequestError, EntityNotExistsError, IOException {

        this.throwOnNullRequest(request);
        if (!request.isSetPath()) {
            throw new BadRequestError("request missing 'path' parameter");
        }

        ThriftRequest<deleteDestination_args> thriftRequest = null;
        ThriftResponse<deleteDestination_result> thriftResponse = null;
        try {
            thriftRequest = buildThriftRequest("deleteDestination", new deleteDestination_args(request));
            thriftResponse = doRemoteCall(thriftRequest);
            deleteDestination_result result = thriftResponse.getBody(deleteDestination_result.class);
            if (thriftResponse.getResponseCode() == ResponseCode.OK) {
                return;
            }
            if (result != null && result.isSetEntityError()) {
                throw result.getEntityError();
            }
            if (result != null && result.isSetRequestError()) {
                throw result.getRequestError();
            }
            throw new IOException("deleteDestination failed with unknown error:" + result);
        } finally {
            if (thriftResponse != null) {
                thriftResponse.release();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerGroupDescription readConsumerGroup(ReadConsumerGroupRequest request)
            throws EntityNotExistsError, BadRequestError, IOException {

        this.throwOnNullRequest(request);

        if (!request.isSetConsumerGroupName()) {
            throw new BadRequestError("request missing consumerGroupName parameter");
        }
        if (!request.isSetDestinationPath()) {
            throw new BadRequestError("request missing destinationPath parameter");
        }

        ThriftRequest<readConsumerGroup_args> thriftRequest = null;
        ThriftResponse<readConsumerGroup_result> thriftResponse = null;
        try {
            thriftRequest = buildThriftRequest("readConsumerGroup", new readConsumerGroup_args(request));
            thriftResponse = doRemoteCall(thriftRequest);
            readConsumerGroup_result result = thriftResponse.getBody(readConsumerGroup_result.class);
            if (thriftResponse.getResponseCode() == ResponseCode.OK) {
                return result.getSuccess();
            }
            if (result != null && result.isSetEntityError()) {
                throw result.getEntityError();
            }
            if (result != null && result.isSetRequestError()) {
                throw result.getRequestError();
            }

            throw new IOException("readConsumerGroup failed with unknown error:" + result);
        } finally {
            if (thriftResponse != null) {
                thriftResponse.release();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DestinationDescription readDestination(ReadDestinationRequest request)
            throws BadRequestError, EntityNotExistsError, IOException {

        this.throwOnNullRequest(request);
        if (!request.isSetPath()) {
            throw new BadRequestError("request missing 'path' parameter");
        }

        ThriftRequest<readDestination_args> thriftRequest = null;
        ThriftResponse<readDestination_result> thriftResponse = null;
        try {
            thriftRequest = buildThriftRequest("readDestination", new readDestination_args(request));
            thriftResponse = doRemoteCall(thriftRequest);
            readDestination_result result = thriftResponse.getBody(readDestination_result.class);
            if (thriftResponse.getResponseCode() == ResponseCode.OK) {
                return result.getSuccess();
            }
            if (result != null && result.isSetEntityError()) {
                throw result.getEntityError();
            }
            if (result != null && result.isSetRequestError()) {
                throw result.getRequestError();
            }
            throw new IOException("readDestination failed with unknown error:" + result);
        } finally {
            if (thriftResponse != null) {
                thriftResponse.release();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerGroupDescription updateConsumerGroup(UpdateConsumerGroupRequest request)
            throws EntityNotExistsError, BadRequestError, IOException {

        this.throwOnNullRequest(request);

        if (!request.isSetConsumerGroupName()) {
            throw new BadRequestError("request missing consumerGroupName parameter");
        }
        if (!request.isSetDestinationPath()) {
            throw new BadRequestError("request missing destinationPath parameter");
        }

        ThriftRequest<updateConsumerGroup_args> thriftRequest = null;
        ThriftResponse<updateConsumerGroup_result> thriftResponse = null;
        try {
            thriftRequest = buildThriftRequest("updateConsumerGroup", new updateConsumerGroup_args(request));
            thriftResponse = doRemoteCall(thriftRequest);
            updateConsumerGroup_result result = thriftResponse.getBody(updateConsumerGroup_result.class);
            if (thriftResponse.getResponseCode() == ResponseCode.OK) {
                return result.getSuccess();
            }
            if (result != null && result.isSetEntityError()) {
                throw result.getEntityError();
            }
            if (result != null && result.isSetRequestError()) {
                throw result.getRequestError();
            }

            throw new IOException("updateConsumerGroup failed with unknown error:" + result);
        } finally {
            if (thriftResponse != null) {
                thriftResponse.release();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DestinationDescription updateDestination(UpdateDestinationRequest request)
            throws BadRequestError, EntityNotExistsError, IOException {

        this.throwOnNullRequest(request);
        if (!request.isSetPath()) {
            throw new BadRequestError("request missing 'path' parameter");
        }

        ThriftRequest<updateDestination_args> thriftRequest = null;
        ThriftResponse<updateDestination_result> thriftResponse = null;
        try {
            thriftRequest = buildThriftRequest("updateDestination", new updateDestination_args(request));
            thriftResponse = doRemoteCall(thriftRequest);
            updateDestination_result result = thriftResponse.getBody(updateDestination_result.class);
            if (thriftResponse.getResponseCode() == ResponseCode.OK) {
                return result.getSuccess();
            }
            if (result != null && result.isSetEntityError()) {
                throw result.getEntityError();
            }
            if (result != null && result.isSetRequestError()) {
                throw result.getRequestError();
            }
            throw new IOException("updateDestination failed with unknown error:" + result);
        } finally {
            if (thriftResponse != null) {
                thriftResponse.release();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListConsumerGroupResult listConsumerGroups(ListConsumerGroupRequest request)
            throws BadRequestError, IOException {

        this.throwOnNullRequest(request);
        if (!request.isSetDestinationPath()) {
            throw new BadRequestError("request missing destinationPath parameter");
        }

        if (request.getLimit() <= 0) {
            request.setLimit(DEFAULT_PAGE_SIZE);
        }

        ThriftRequest<listConsumerGroups_args> thriftRequest = null;
        ThriftResponse<listConsumerGroups_result> thriftResponse = null;
        try {
            thriftRequest = buildThriftRequest("listConsumerGroups", new listConsumerGroups_args(request));
            thriftResponse = doRemoteCall(thriftRequest);
            listConsumerGroups_result result = thriftResponse.getBody(listConsumerGroups_result.class);
            if (thriftResponse.getResponseCode() == ResponseCode.OK) {
                return result.getSuccess();
            }
            if (result != null && result.isSetRequestError()) {
                throw result.getRequestError();
            }
            throw new IOException("listConsumerGroups failed with unknown error:" + result);
        } finally {
            if (thriftResponse != null) {
                thriftResponse.release();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListDestinationsResult listDestinations(ListDestinationsRequest request)
            throws BadRequestError, IOException {

        this.throwOnNullRequest(request);

        if (request.getLimit() <= 0) {
            request.setLimit(DEFAULT_PAGE_SIZE);
        }

        ThriftRequest<listDestinations_args> thriftRequest = null;
        ThriftResponse<listDestinations_result> thriftResponse = null;
        try {
            thriftRequest = buildThriftRequest("listDestinations", new listDestinations_args(request));
            thriftResponse = doRemoteCall(thriftRequest);
            listDestinations_result result = thriftResponse.getBody(listDestinations_result.class);
            if (thriftResponse.getResponseCode() == ResponseCode.OK) {
                return result.getSuccess();
            }
            if (result != null && result.isSetRequestError()) {
                throw result.getRequestError();
            }
            throw new IOException("listDestinations failed with unknown error:" + result);
        } finally {
            if (thriftResponse != null) {
                thriftResponse.release();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<HostAddress> readDestinationHosts(String path)
            throws BadRequestError, EntityDisabledError, EntityNotExistsError, IOException {

        if (path.isEmpty()) {
            throw new IllegalArgumentException("path cannot be empty");
        }

        ThriftRequest<readDestinationHosts_args> thriftRequest = null;
        ThriftResponse<readDestinationHosts_result> thriftResponse = null;
        try {
            ReadDestinationHostsRequest request = new ReadDestinationHostsRequest();
            request.setPath(path);
            thriftRequest = buildThriftRequest("readDestinationHosts", new readDestinationHosts_args(request));
            thriftResponse = doRemoteCall(thriftRequest);
            readDestinationHosts_result result = thriftResponse.getBody(readDestinationHosts_result.class);
            if (thriftResponse.getResponseCode() == ResponseCode.OK) {
                return result.getSuccess().getHostAddresses();
            }
            if (result != null && result.isSetRequestError()) {
                throw result.getRequestError();
            }
            if (result != null && result.isSetEntityDisabled()) {
                throw result.getEntityDisabled();
            }
            if (result != null && result.isSetEntityError()) {
                throw result.getEntityError();
            }
            throw new IOException("readDestinationHosts failed with unknown error:" + result);
        } finally {
            if (thriftResponse != null) {
                thriftResponse.release();
            }
        }
    }

    @Override
    public ReadPublisherOptionsResult readPublisherOptions(String path)
            throws EntityNotExistsError, EntityDisabledError, BadRequestError, IOException {

        if (path.isEmpty()) {
            throw new IllegalArgumentException("path cannot be empty");
        }

        ThriftRequest<readPublisherOptions_args> thriftRequest = null;
        ThriftResponse<readPublisherOptions_result> thriftResponse = null;
        try {
            ReadPublisherOptionsRequest request = new ReadPublisherOptionsRequest();
            request.setPath(path);
            thriftRequest = buildThriftRequest("readPublisherOptions", new readPublisherOptions_args(request));
            thriftResponse = doRemoteCall(thriftRequest);
            readPublisherOptions_result result = thriftResponse.getBody(readPublisherOptions_result.class);
            if (thriftResponse.getResponseCode() == ResponseCode.OK) {
                return result.getSuccess();
            }
            if (result != null && result.isSetRequestError()) {
                throw result.getRequestError();
            }
            if (result != null && result.isSetEntityDisabled()) {
                throw result.getEntityDisabled();
            }
            if (result != null && result.isSetEntityError()) {
                throw result.getEntityError();
            }
            throw new IOException("readPublisherOptions failed with unknown error:" + result);
        } finally {
            if (thriftResponse != null) {
                thriftResponse.release();
            }
        }
    }

    @Override
    public List<HostAddress> readConsumerGroupHosts(String path, String consumerGroupName)
            throws BadRequestError, EntityNotExistsError, EntityDisabledError, IOException {

        if (path.isEmpty()) {
            throw new IllegalArgumentException("path cannot be empty");
        }
        if (consumerGroupName.isEmpty()) {
            throw new IllegalArgumentException("consumerGroupName cannot be empty");
        }

        ThriftRequest<readConsumerGroupHosts_args> thriftRequest = null;
        ThriftResponse<readConsumerGroupHosts_result> thriftResponse = null;
        try {
            ReadConsumerGroupHostsRequest request = new ReadConsumerGroupHostsRequest();
            request.setDestinationPath(path);
            request.setConsumerGroupName(consumerGroupName);
            thriftRequest = buildThriftRequest("readConsumerGroupHosts", new readConsumerGroupHosts_args(request));
            thriftResponse = doRemoteCall(thriftRequest);
            readConsumerGroupHosts_result result = thriftResponse.getBody(readConsumerGroupHosts_result.class);
            if (thriftResponse.getResponseCode() == ResponseCode.OK) {
                return result.getSuccess().getHostAddresses();
            }
            if (result != null && result.isSetRequestError()) {
                throw result.getRequestError();
            }
            if (result != null && result.isSetEntityDisabled()) {
                throw result.getEntityDisabled();
            }
            if (result != null && result.isSetEntityError()) {
                throw result.getEntityError();
            }
            throw new IOException("readPublisherOptions failed with unknown error:" + result);
        } finally {
            if (thriftResponse != null) {
                thriftResponse.release();
            }
        }
    }
}
