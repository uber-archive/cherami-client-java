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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

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
import com.uber.cherami.ReadConsumerGroupRequest;
import com.uber.cherami.ReadDestinationRequest;
import com.uber.cherami.ReadPublisherOptionsResult;
import com.uber.cherami.UpdateConsumerGroupRequest;
import com.uber.cherami.UpdateDestinationRequest;

/**
 * CheramiClient is the client side interface for Cherami.
 *
 * All interactions to Cherami from the client must start with the APIs defined
 * in this interface. The tchannel service name that this client talks to is
 * cherami-frontendhost. For service discovery, the cherami client uses
 * hyperbahn, by default. Callers can optionally specify a specific ip:port for
 * connecting directly to a cherami-frontend service. For prod, always use
 * hyperbahn.
 *
 * Examples:
 *
 * Prod client that uses Hyperbahn for discovery:
 *
 * <code>
 *          MetricsClient client = new M3Client(m3Scope);
 *          ClientOptions options = new ClientOptions().setMetricsClient(client);
 *          CheramiClient client = CheramiClient.Builder().setClientOptions(options).build();
 *  </code>
 *
 * Staging client that uses hyperbahn for discovery: <code>
 *          ClientOptions options = new ClientOptions();
 *          options.setMetricsClient(new M3Client(m3Scope));
 *          options.setDeploymentStr("staging");
 *          CheramiClient client = CheramiClient.Builder().setClientOptions(options).build();
 *  </code>
 *
 * Staging client that uses a specific ip:port: <code>
 *          ClientOptions options = new ClientOptions();
 *          options.setMetricsClient(new M3Client(m3Scope));
 *          options.setDeploymentStr("staging");
 *          CheramiClient client = CheramiClient.Builder(host, port).setClientOptions(options).build();
 *      </code>
 *
 */
public interface CheramiClient extends Closeable {
    /**
     * Creates a consumer group for the given destination with the given name.
     *
     * @param request
     *            Options with which to create ConsumerGroup REQUIRED:
     *            request.ConsumerGroupName, request.DestinationPath
     * @return ConsumerGroupDescription on success.
     * @throws EntityAlreadyExistsError
     *             If a consumer group with the requested name already exists
     *             for the destination.
     * @throws BadRequestError
     *             If the request parameters are invalid.
     * @throws IOException
     *             If there is a network I/O error.
     */
    ConsumerGroupDescription createConsumerGroup(CreateConsumerGroupRequest request)
            throws EntityAlreadyExistsError, BadRequestError, IOException;

    /**
     * Creates a cherami destination with the given name, if it already doesn't
     * exist.
     *
     * @param request
     *            Options with which to create Destination REQUIRED:
     *            request.DestinationPath
     * @return DestinationDescription, on success.
     * @throws EntityAlreadyExistsError
     *             If a destination already exists with the requested name.
     * @throws BadRequestError
     *             If the request parameters are invalid.
     * @throws IOException
     *             On a network I/O Error.
     */
    DestinationDescription createDestination(CreateDestinationRequest request)
            throws EntityAlreadyExistsError, BadRequestError, IOException;

    /**
     * Creates a publisher to a requested destination.
     *
     * The returned publisher can be used to publish messages to a cherami
     * destination. Internally, the publisher maintains a connection pool to
     * cherami backend servers and multiplexes incoming messages among the
     * backend connections. Its a best practice to create at-most one publisher
     * per process. This call MUST be followed by publisher.open() before
     * messages can be published.
     *
     * Publisher Options:
     *
     * maxInflightMessagesPerConnection: This value represents the maximum
     * number of messages that remain unacked from the cherami servers, before
     * the application receives a publish error. This value is per connection
     * and be aware that a single destination can have multiple connections to
     * the cherami servers (typically 4). This value will directly impact the
     * max publish throughput achievable. The default value for this option is
     * 1K.
     *
     * bufferSizeBytes: This value represents the send buffer size for the
     * publisher. There can be at-most bufferSizeBytes worth of data that can be
     * queued by the publisher for sending to the cherami servers later on. Apps
     * will receive a publish error when this value is exceeded. The default
     * value for this option is 1MB.
     *
     * writeTimeoutMillis: This value represents the amount of time after which
     * publisher will timeout sending a message. When this happens, callers are
     * expected backoff and retry. Default value is 1minute.
     *
     * @param request
     *            Request params for creating a cherami publisher. Destination
     *            path is required.
     *
     * @return CheramiPublisher that can be used to publish messages.
     */
    CheramiPublisher createPublisher(CreatePublisherRequest request);

    /**
     * Create a consumer for a requested destination and consumer group.
     *
     * The returned consumer can be used consume messages from a cherami
     * destination. Internally, the consumer maintains a connection pool to
     * cherami backend servers and multiplexes messages received from multiple
     * connections to the consumer. Its a best practice to create at-most one
     * consumer per process. This call MUST be followed by consumer.open()
     * before messages can be consumed.
     *
     * Consumer Options:
     *
     * prefetchCount: Receive buffer size, represents number of messages that
     * can be buffered locally, before delivering to the application. This value
     * directly impacts consumer throughput. Default value is 1K.
     *
     * @param request
     *            Request params for building a cherami consumer. Path and
     *            consumerGroupName are required.
     * @return CheramiConsumer object.
     */
    CheramiConsumer createConsumer(CreateConsumerRequest request);

    /**
     * Delete a ConsumerGroup associated with a destination.
     *
     * @param request
     *            Request params, destinationPath and consumerGroupName are
     *            required arguments to this method.
     * @throws EntityNotExistsError
     *             If the consumer group does not exist for the given
     *             destination.
     * @throws BadRequestError
     *             If the request parameters are invalid.
     * @throws IOException
     *             On a network I/O error.
     */
    void deleteConsumerGroup(DeleteConsumerGroupRequest request)
            throws EntityNotExistsError, BadRequestError, IOException;

    /**
     * Deletes a destination identified by the given path, if it exists.
     *
     * @param request
     *            Request params, path is required.
     * @throws EntityNotExistsError
     *             If the destination with the provided path does not exist.
     * @throws BadRequestError
     *             If the request parameters are invalid.
     * @throws IOException
     *             On a network I/O error.
     */
    void deleteDestination(DeleteDestinationRequest request) throws EntityNotExistsError, BadRequestError, IOException;

    /**
     * Returns the metadata associated with the given destination / consumer
     * group.
     *
     * @param request
     *            Request params, destinationPath and consumerGroupName are
     *            required arguments to this method.
     *
     * @return ConsumerGroupDescription for the given consumer group.
     *
     * @throws EntityNotExistsError
     *             If the consumer group does not exist for the given
     *             destination.
     * @throws BadRequestError
     *             If the request parameters are invalid.
     * @throws IOException
     *             On a network I/O error.
     */
    ConsumerGroupDescription readConsumerGroup(ReadConsumerGroupRequest request)
            throws EntityNotExistsError, BadRequestError, IOException;

    /**
     * Returns the metadata associated with the given destination.
     *
     * @param request
     *            Request params, path is required.
     *
     * @return DestinationDescription for the requested destination.
     *
     * @throws EntityNotExistsError
     *             If the destination with the provided path does not exist.
     * @throws BadRequestError
     *             If the request parameters are invalid.
     * @throws IOException
     *             On a network I/O error.
     */
    DestinationDescription readDestination(ReadDestinationRequest request)
            throws EntityNotExistsError, BadRequestError, IOException;

    /**
     * Update the consumer group metadata associated with the given destination
     * / consumergroup.
     *
     * @param request
     *            Request params, destinationPath and consumerGroupName are
     *            required arguments to this method.
     *
     * @return Updated ConsumerGroupDescription for the given consumer group.
     *
     * @throws EntityNotExistsError
     *             If the consumer group does not exist for the given
     *             destination.
     * @throws BadRequestError
     *             If the request parameters are invalid.
     * @throws IOException
     *             On a network I/O error.
     */
    ConsumerGroupDescription updateConsumerGroup(UpdateConsumerGroupRequest request)
            throws EntityNotExistsError, BadRequestError, IOException;

    /**
     * Updates the metadata associated with the given destination.
     *
     * @param request
     *            Request params, path is required.
     *
     * @return Updated DestinationDescription for the requested destination.
     *
     * @throws EntityNotExistsError
     *             If the destination with the provided path does not exist.
     * @throws BadRequestError
     *             If the request parameters are invalid.
     * @throws IOException
     *             On a network I/O error.
     */
    DestinationDescription updateDestination(UpdateDestinationRequest request)
            throws EntityNotExistsError, BadRequestError, IOException;

    /**
     * Lists all consumer groups for a given destination.
     *
     * This is a paginated API, callers must continue to call this API as long
     * as the returned pageToken is non-zero. Except for the first API call,
     * subsequent calls must always propagate the pageToken returned from the
     * previous call.
     *
     * @param request
     *            Request params, destinationPath and consumerGroup are
     *            required.
     *
     * @return ListConsumerGroupResult Contains the list of consumer groups and
     *         the pageToken to be passed along in the next call.
     *
     * @throws BadRequestError
     *             If the request parameters are invalid.
     * @throws IOException
     *             On a network I/O error.
     */
    ListConsumerGroupResult listConsumerGroups(ListConsumerGroupRequest request) throws BadRequestError, IOException;

    /**
     * List all destinations matching the given prefix path.
     *
     * This is a paginated API, callers must continue to call this API as long
     * as the returned pageToken is non-zero. Except for the first API call,
     * subsequent calls must always propagate the pageToken returned from the
     * previous call.
     *
     * @param request
     *            Request params, prefix and pageToken are required.
     *
     * @return ListDestinationsResult containing list of destinations and
     *         nextPageToken.
     *
     * @throws BadRequestError
     *             If the request parameters are invalid.
     * @throws IOException
     *             On a network I/O error.
     */
    ListDestinationsResult listDestinations(ListDestinationsRequest request) throws BadRequestError, IOException;

    /**
     * Returns the publish streaming endpoints for a given destination.
     *
     * @param path
     *            The destination path for which endpoints needs to be found.
     * @return List of HostAddresses representing the streaming endpoints.
     * @throws BadRequestError
     *             If the request is bad.
     * @throws EntityDisabledError
     *             If the destination status is not enabled.
     * @throws EntityNotExistsError
     *             If the destination no longer exist.
     * @throws IOException
     *             Any other I/O error.
     */
    List<HostAddress> readDestinationHosts(String path)
            throws BadRequestError, EntityDisabledError, EntityNotExistsError, IOException;

    /**
     * Returns the publish streaming endpoints for a given destination.
     *
     * This method returns a list of of publish endpoints along with the list of
     * streaming protocols supported by the server.
     *
     * @param path
     *            Destination path for which endpoints is desired.
     * @return List of host addresses and supported protocols.
     * @throws EntityNotExistsError
     *             If the destination does not exist.
     * @throws EntityDisabledError
     *             If the destination is disabled.
     * @throws BadRequestError
     *             If the request is bad.
     * @throws IOException
     *             On a network I/O error.
     */
    ReadPublisherOptionsResult readPublisherOptions(String path)
            throws EntityNotExistsError, EntityDisabledError, BadRequestError, IOException;

    /**
     * Returns the endpoints from which messages can be streamed/consumed.
     *
     * @param path
     *            Destination path.
     * @param consumerGroupName
     *            Consumer group name that needs to receive messages.
     * @return List of host addresses, representing the streaming endpoints.
     * @throws BadRequestError
     *             If the request is bad.
     * @throws EntityNotExistsError
     *             If the destination does not exist.
     * @throws EntityDisabledError
     *             If the destination is disabled.
     * @throws IOException
     *             Any other network I/O error.
     */
    List<HostAddress> readConsumerGroupHosts(String path, String consumerGroupName)
            throws BadRequestError, EntityNotExistsError, EntityDisabledError, IOException;

    /**
     * @return Returns the client options that this client was built with.
     */
    ClientOptions getOptions();

    /**
     * Builder that builds a CheramiClient object.
     *
     * The returned CheramiClient can either automatically discover the cherami
     * servers by using hyperbahan or callers can provide a specific ip:port for
     * a cherami-frontend server.
     *
     * For production, always use hyperbahn for discovery.
     */
    class Builder {

        private static final String DEFAULT_ROUTER_FILE = "/etc/uber/hyperbahn/hosts.json";

        private String host = "";
        private int port;
        private String routerFile = DEFAULT_ROUTER_FILE;
        private ClientOptions options = new ClientOptions.Builder().build();

        /**
         * Returns a Builder object.
         *
         * @param host
         *            cherami-frontend service host
         * @param port
         *            cherami-frontend service port
         */
        public Builder(String host, int port) {
            this.host = host;
            this.port = port;
        }

        /**
         * Returns a builder with default options.
         */
        public Builder() {
        }

        /**
         * Sets the path where the hyperbahn router file can be found. This
         * option is only necessary when not explicity specifying a host:port
         * for cherami-frontend.
         *
         * @param routerFile
         *            String, representing the absolute path.
         * @return Builder object.
         */
        public Builder setRouterFile(String routerFile) {
            this.routerFile = routerFile;
            return this;
        }

        /**
         * Sets the CheramiClient options.
         *
         * @param options
         *            ClientOptions for cherami client.
         * @return Builder object.
         */
        public Builder setClientOptions(ClientOptions options) {
            this.options = options;
            return this;
        }

        /**
         * Builds and returns a CheramiClient object.
         *
         * @return CheramiClient object.
         * @throws IOException
         *             If there is an I/O error while buiding the object.
         */
        public CheramiClientImpl build() throws IOException {
            if (host != null && !host.isEmpty()) {
                return new CheramiClientImpl(host, port, options);
            }
            return new CheramiClientImpl(routerFile, options);
        }

        protected String getHost() {
            return this.host;
        }

        protected int getPort() {
            return this.port;
        }

        protected String getRouterFile() {
            return this.routerFile;
        }
    }
}
