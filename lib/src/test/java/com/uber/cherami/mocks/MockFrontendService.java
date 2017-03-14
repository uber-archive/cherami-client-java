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
package com.uber.cherami.mocks;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;

import com.uber.cherami.BFrontend;
import com.uber.cherami.BOut;
import com.uber.cherami.BadRequestError;
import com.uber.cherami.ChecksumOption;
import com.uber.cherami.ConsumerGroupDescription;
import com.uber.cherami.DestinationDescription;
import com.uber.cherami.EntityAlreadyExistsError;
import com.uber.cherami.EntityNotExistsError;
import com.uber.cherami.HostAddress;
import com.uber.cherami.ListConsumerGroupResult;
import com.uber.cherami.ListDestinationsResult;
import com.uber.cherami.PutMessage;
import com.uber.cherami.ReadConsumerGroupHostsResult;
import com.uber.cherami.ReadDestinationHostsResult;
import com.uber.cherami.ReadPublisherOptionsResult;
import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.handlers.ThriftRequestHandler;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;

/**
 * A mock frontend for testing
 */
public class MockFrontendService {
    private static final String CLIENT_NAME = "CheramiClient";
    private static final String DEFAULT_SERVICE_NAME = "cherami-frontendhost";
    /**
     * A Map to hold consumerGroup key-value pairs. Used as a way to store values in between calls (i.e. creation and deletion).
     */
    private static final HashMap<String, String> consumerGroupMap = new HashMap<String, String>();
    /**
     * A Map to hold destination key-value pairs. Used as a way to store values in between calls (i.e. creation and deletion).
     */
    private static final HashMap<String, String> destinationMap = new HashMap<String, String>();
    /**
     * A map to hold the messageBatches for a specific destination
     */
    private static final HashMap<String, List<PutMessage>> messageMap = new HashMap<String, List<PutMessage>>();

    private static TChannel tChannel;
    private static final ReadDestinationHostsHandler destinationHostsHandler = new ReadDestinationHostsHandler();
    private static final ReadConsumerGroupHostsHandler consumerGroupHostsHandler = new ReadConsumerGroupHostsHandler();
    private static final ReadPublisherOptionsHandler publisherOptionsHandler = new ReadPublisherOptionsHandler();

    public static void createServer() throws Exception {
        //Create TChannel
        tChannel = new TChannel.Builder(CLIENT_NAME)
                .setServerHost(InetAddress.getByName("127.0.0.1")).setServerPort(0).build();
        // create sub channel to handle requests
        tChannel.makeSubChannel(DEFAULT_SERVICE_NAME)
                .register("BFrontend::createConsumerGroup", new CreateConsumerGroupHandler())
                .register("BFrontend::createDestination", new CreateDestinationHandler())
                .register("BFrontend::deleteConsumerGroup", new DeleteConsumerGroupHandler())
                .register("BFrontend::deleteDestination", new DeleteDestinationHandler())
                .register("BFrontend::readConsumerGroup", new ReadConsumerGroupHandler())
                .register("BFrontend::readDestination", new ReadDestinationHandler())
                .register("BFrontend::updateConsumerGroup", new UpdateConsumerGroupHandler())
                .register("BFrontend::updateDestination", new UpdateDestinationHandler())
                .register("BFrontend::listDestinations", new ListDestinationsHandler())
                .register("BFrontend::listConsumerGroups", new ListConsumerGroupsHandler())
                .register("BFrontend::readDestinationHosts", destinationHostsHandler)
                .register("BFrontend::readConsumerGroupHosts", consumerGroupHostsHandler)
                .register("BFrontend::readPublisherOptions", publisherOptionsHandler);

        tChannel.listen();
    }


    public TChannel startOutputTChannel(String host, int port) throws Exception {
        //Create TChannel
        TChannel outputTChannel = new TChannel.Builder("CheramiClient")
                .setServerHost(InetAddress.getByName(host)).setServerPort(port).build();
        // create sub channel to handle requests
        outputTChannel.makeSubChannel("cherami-outputhost")
                .register("BOut::ackMessages", new AckMessagesHandler());

        return outputTChannel;
    }

    public static void setHosts(List<HostAddress> destinationHosts, List<HostAddress> consumerHosts) {
        destinationHostsHandler.setDestinationHosts(destinationHosts);
        publisherOptionsHandler.setDestinationHosts(destinationHosts);
        consumerGroupHostsHandler.setConsumerGroupHosts(consumerHosts);
    }

    public int getPort() {
        return tChannel.getListeningPort();
    }

    public void close() {
        tChannel.shutdown();
    }

    public void clearMaps() {
        consumerGroupMap.clear();
        destinationMap.clear();
        messageMap.clear();
    }

    private static class CreateConsumerGroupHandler extends ThriftRequestHandler<BFrontend.createConsumerGroup_args, BFrontend.createConsumerGroup_result> {
        @Override
        public ThriftResponse<BFrontend.createConsumerGroup_result> handleImpl(ThriftRequest<BFrontend.createConsumerGroup_args> request) {
            String groupName = request.getBody(BFrontend.createConsumerGroup_args.class).getRegisterRequest().getConsumerGroupName();
            String path = request.getBody(BFrontend.createConsumerGroup_args.class).getRegisterRequest().getDestinationPath();

            ConsumerGroupDescription description = new ConsumerGroupDescription();

            BFrontend.createConsumerGroup_result res = new BFrontend.createConsumerGroup_result();
            ThriftResponse<BFrontend.createConsumerGroup_result> response;

            if (consumerGroupMap.containsValue(groupName) && consumerGroupMap.containsValue(path)) {
                res.setEntityExistsError(new EntityAlreadyExistsError("EntityAlreadyExists"));
                response = new ThriftResponse.Builder<BFrontend.createConsumerGroup_result>(request).setBody(res).setResponseCode(ResponseCode.Error).build();
            } else {
                consumerGroupMap.put("groupName", groupName);
                consumerGroupMap.put("path", path);
                description.setConsumerGroupName(groupName);
                description.setDestinationPath(path);
                res.setSuccess(description);
                response = new ThriftResponse.Builder<BFrontend.createConsumerGroup_result>(request).setBody(res).build();
            }

            return response;
        }
    }

    private static class CreateDestinationHandler extends ThriftRequestHandler<BFrontend.createDestination_args, BFrontend.createDestination_result> {
        @Override
        public ThriftResponse<BFrontend.createDestination_result> handleImpl(ThriftRequest<BFrontend.createDestination_args> request) {
            String email = request.getBody(BFrontend.createDestination_args.class).getCreateRequest().getOwnerEmail();
            String path = request.getBody(BFrontend.createDestination_args.class).getCreateRequest().getPath();

            DestinationDescription description = new DestinationDescription();
            BFrontend.createDestination_result res = new BFrontend.createDestination_result();
            ThriftResponse<BFrontend.createDestination_result> response;

            if (destinationMap.containsValue(email) && destinationMap.containsValue(path)) {
                res.setEntityExistsError(new EntityAlreadyExistsError("EntityAlreadyExists"));
                response = new ThriftResponse.Builder<BFrontend.createDestination_result>(request).setBody(res).setResponseCode(ResponseCode.Error).build();
            } else {
                destinationMap.put("email", email);
                destinationMap.put("path", path);
                description.setOwnerEmail(email);
                description.setPath(path);
                res.setSuccess(description);
                response = new ThriftResponse.Builder<BFrontend.createDestination_result>(request).setBody(res).build();
            }

            return response;
        }
    }

    private static class DeleteConsumerGroupHandler extends ThriftRequestHandler<BFrontend.deleteConsumerGroup_args, BFrontend.deleteConsumerGroup_result> {
        @Override
        public ThriftResponse<BFrontend.deleteConsumerGroup_result> handleImpl(ThriftRequest<BFrontend.deleteConsumerGroup_args> request) {
            String groupName = request.getBody(BFrontend.deleteConsumerGroup_args.class).getDeleteRequest().getConsumerGroupName();
            String path = request.getBody(BFrontend.deleteConsumerGroup_args.class).getDeleteRequest().getDestinationPath();

            BFrontend.deleteConsumerGroup_result res = new BFrontend.deleteConsumerGroup_result();
            ThriftResponse<BFrontend.deleteConsumerGroup_result> response;

            if (!consumerGroupMap.containsValue(groupName) && !consumerGroupMap.containsValue(path)) {
                res.setEntityError(new EntityNotExistsError("EntityDoesNotExist"));
                response = new ThriftResponse.Builder<BFrontend.deleteConsumerGroup_result>(request).setBody(res).setResponseCode(ResponseCode.Error).build();
            } else {
                consumerGroupMap.remove("groupName");
                consumerGroupMap.remove("path");
                response = new ThriftResponse.Builder<BFrontend.deleteConsumerGroup_result>(request).setBody(res).build();
            }

            return response;
        }
    }

    private static class DeleteDestinationHandler extends ThriftRequestHandler<BFrontend.deleteDestination_args, BFrontend.deleteDestination_result> {
        @Override
        public ThriftResponse<BFrontend.deleteDestination_result> handleImpl(ThriftRequest<BFrontend.deleteDestination_args> request) {
            String path = request.getBody(BFrontend.deleteDestination_args.class).getDeleteRequest().getPath();

            BFrontend.deleteDestination_result res = new BFrontend.deleteDestination_result();
            ThriftResponse<BFrontend.deleteDestination_result> response;

            if (!destinationMap.containsValue(path)) {
                res.setEntityError(new EntityNotExistsError("EntityDoesNotExist"));
                response = new ThriftResponse.Builder<BFrontend.deleteDestination_result>(request).setBody(res).setResponseCode(ResponseCode.Error).build();
            } else {
                destinationMap.clear();
                response = new ThriftResponse.Builder<BFrontend.deleteDestination_result>(request).setBody(res).build();
            }

            return response;

        }
    }

    private static class ReadConsumerGroupHandler extends ThriftRequestHandler<BFrontend.readConsumerGroup_args, BFrontend.readConsumerGroup_result> {
        @Override
        public ThriftResponse<BFrontend.readConsumerGroup_result> handleImpl(ThriftRequest<BFrontend.readConsumerGroup_args> request) {
            String consumerName = request.getBody(BFrontend.readConsumerGroup_args.class).getGetRequest().getConsumerGroupName();
            String path = request.getBody(BFrontend.readConsumerGroup_args.class).getGetRequest().getDestinationPath();

            ConsumerGroupDescription description = new ConsumerGroupDescription();
            BFrontend.readConsumerGroup_result res = new BFrontend.readConsumerGroup_result();
            ThriftResponse<BFrontend.readConsumerGroup_result> response;

            if (consumerGroupMap.get("groupName").equals(consumerName) && consumerGroupMap.get("path").equals(path)) {
                description.setDestinationPath(consumerGroupMap.get("path"));
                description.setConsumerGroupName(consumerGroupMap.get("groupName"));
                res.setSuccess(description);
                response = new ThriftResponse.Builder<BFrontend.readConsumerGroup_result>(request).setBody(res).build();
            } else {
                res.setEntityError(new EntityNotExistsError("EntityDoesNotExist"));
                response = new ThriftResponse.Builder<BFrontend.readConsumerGroup_result>(request).setBody(res).setResponseCode(ResponseCode.Error).build();
            }

            return response;
        }
    }

    private static class ReadDestinationHandler extends ThriftRequestHandler<BFrontend.readDestination_args, BFrontend.readDestination_result> {
        @Override
        public ThriftResponse<BFrontend.readDestination_result> handleImpl(ThriftRequest<BFrontend.readDestination_args> request) {
            String path = request.getBody(BFrontend.readDestination_args.class).getGetRequest().getPath();

            DestinationDescription description = new DestinationDescription();
            BFrontend.readDestination_result res = new BFrontend.readDestination_result();
            ThriftResponse<BFrontend.readDestination_result> response;

            if (destinationMap.get("path").equals(path)) {
                description.setPath(destinationMap.get("path"));
                description.setOwnerEmail(destinationMap.get("email"));
                res.setSuccess(description);
                response = new ThriftResponse.Builder<BFrontend.readDestination_result>(request).setBody(res).build();
            } else {
                res.setEntityError(new EntityNotExistsError("EntityDoesNotExist"));
                response = new ThriftResponse.Builder<BFrontend.readDestination_result>(request).setBody(res).setResponseCode(ResponseCode.Error).build();
            }

            return response;
        }
    }

    private static class UpdateConsumerGroupHandler extends ThriftRequestHandler<BFrontend.updateConsumerGroup_args, BFrontend.updateConsumerGroup_result> {
        @Override
        public ThriftResponse<BFrontend.updateConsumerGroup_result> handleImpl(ThriftRequest<BFrontend.updateConsumerGroup_args> request) {
            String consumerName = request.getBody(BFrontend.updateConsumerGroup_args.class).getUpdateRequest().getConsumerGroupName();
            String path = request.getBody(BFrontend.updateConsumerGroup_args.class).getUpdateRequest().getDestinationPath();

            ConsumerGroupDescription description = new ConsumerGroupDescription();
            BFrontend.updateConsumerGroup_result res = new BFrontend.updateConsumerGroup_result();
            ThriftResponse<BFrontend.updateConsumerGroup_result> response;

            if (consumerGroupMap.get("groupName").equals(consumerName)) {
                consumerGroupMap.put("path", path);
                description.setDestinationPath(consumerGroupMap.get("path"));
                description.setConsumerGroupName(consumerGroupMap.get("groupName"));
                res.setSuccess(description);
                response = new ThriftResponse.Builder<BFrontend.updateConsumerGroup_result>(request).setBody(res).build();
            } else {
                res.setEntityError(new EntityNotExistsError("EntityDoesNotExist"));
                response = new ThriftResponse.Builder<BFrontend.updateConsumerGroup_result>(request).setBody(res).setResponseCode(ResponseCode.Error).build();
            }

            return response;
        }
    }

    private static class UpdateDestinationHandler extends ThriftRequestHandler<BFrontend.updateDestination_args, BFrontend.updateDestination_result> {
        @Override
        public ThriftResponse<BFrontend.updateDestination_result> handleImpl(ThriftRequest<BFrontend.updateDestination_args> request) {
            String email = request.getBody(BFrontend.updateDestination_args.class).getUpdateRequest().getOwnerEmail();
            String path = request.getBody(BFrontend.updateDestination_args.class).getUpdateRequest().getPath();

            DestinationDescription description = new DestinationDescription();
            BFrontend.updateDestination_result res = new BFrontend.updateDestination_result();
            ThriftResponse<BFrontend.updateDestination_result> response;

            if (destinationMap.get("path").equals(path)) {
                destinationMap.put("email", email);
                description.setOwnerEmail(destinationMap.get("email"));
                description.setPath(destinationMap.get("path"));
                res.setSuccess(description);
                response = new ThriftResponse.Builder<BFrontend.updateDestination_result>(request).setBody(res).build();
            } else {
                res.setEntityError(new EntityNotExistsError("EntityDoesNotExist"));
                response = new ThriftResponse.Builder<BFrontend.updateDestination_result>(request).setBody(res).setResponseCode(ResponseCode.Error).build();
            }

            return response;
        }
    }

    private static class ListDestinationsHandler extends ThriftRequestHandler<BFrontend.listDestinations_args, BFrontend.listDestinations_result> {
        @Override
        public ThriftResponse<BFrontend.listDestinations_result> handleImpl(ThriftRequest<BFrontend.listDestinations_args> request) {
            String prefix = request.getBody(BFrontend.listDestinations_args.class).getListRequest().getPrefix();

            ListDestinationsResult description = new ListDestinationsResult();
            BFrontend.listDestinations_result res = new BFrontend.listDestinations_result();

            ThriftResponse<BFrontend.listDestinations_result> response;

            if (destinationMap.get("path").startsWith(prefix)) {
                res.setSuccess(description);
                response = new ThriftResponse.Builder<BFrontend.listDestinations_result>(request).setBody(res).build();
            } else {
                res.setRequestError(new BadRequestError("BadRequest"));
                response = new ThriftResponse.Builder<BFrontend.listDestinations_result>(request)
                        .setResponseCode(ResponseCode.Error).setBody(res).build();
            }

            return response;
        }
    }

    private static class ListConsumerGroupsHandler extends ThriftRequestHandler<BFrontend.listConsumerGroups_args, BFrontend.listConsumerGroups_result> {
        @Override
        public ThriftResponse<BFrontend.listConsumerGroups_result> handleImpl(ThriftRequest<BFrontend.listConsumerGroups_args> request) {
            String path = request.getBody(BFrontend.listConsumerGroups_args.class).getListRequest().getDestinationPath();
            String groupName = request.getBody(BFrontend.listConsumerGroups_args.class).getListRequest().getConsumerGroupName();

            ListConsumerGroupResult description = new ListConsumerGroupResult();
            BFrontend.listConsumerGroups_result res = new BFrontend.listConsumerGroups_result();
            ThriftResponse<BFrontend.listConsumerGroups_result> response;

            if (consumerGroupMap.get("path").equals(path) && consumerGroupMap.get("groupName").equals(groupName)) {
                res.setSuccess(description);
                response = new ThriftResponse.Builder<BFrontend.listConsumerGroups_result>(request).setBody(res).build();
            } else {
                res.setRequestError(new BadRequestError("BadRequest"));
                response = new ThriftResponse.Builder<BFrontend.listConsumerGroups_result>(request).setBody(res).setResponseCode(ResponseCode.Error).build();
            }

            return response;
        }
    }


    private static class ReadDestinationHostsHandler extends ThriftRequestHandler<BFrontend.readDestinationHosts_args, BFrontend.readDestinationHosts_result> {
        List<HostAddress> hosts;

        public void setDestinationHosts(List<HostAddress> newHosts) {
            hosts = newHosts;
        }

        @Override
        public ThriftResponse<BFrontend.readDestinationHosts_result> handleImpl(ThriftRequest<BFrontend.readDestinationHosts_args> request) {
            String path = request.getBody(BFrontend.readDestinationHosts_args.class).getGetHostsRequest().getPath();

            ReadDestinationHostsResult description = new ReadDestinationHostsResult();
            BFrontend.readDestinationHosts_result res = new BFrontend.readDestinationHosts_result();
            ThriftResponse<BFrontend.readDestinationHosts_result> response;

            if (!destinationMap.containsValue(path)) {
                res.setEntityError(new EntityNotExistsError("Entity does not exist"));
                response = new ThriftResponse.Builder<BFrontend.readDestinationHosts_result>(request).setBody(res).setResponseCode(ResponseCode.Error).build();
            } else {
                description.setHostAddresses(hosts);
                res.setSuccess(description);
                response = new ThriftResponse.Builder<BFrontend.readDestinationHosts_result>(request).setBody(res).build();
            }
            return response;
        }
    }

    private static class ReadConsumerGroupHostsHandler extends ThriftRequestHandler<BFrontend.readConsumerGroupHosts_args, BFrontend.readConsumerGroupHosts_result> {
        List<HostAddress> hosts;

        public void setConsumerGroupHosts(List<HostAddress> newHosts) {
            hosts = newHosts;
        }

        @Override
        public ThriftResponse<BFrontend.readConsumerGroupHosts_result> handleImpl(ThriftRequest<BFrontend.readConsumerGroupHosts_args> request) {
            String path = request.getBody(BFrontend.readConsumerGroupHosts_args.class).getGetHostsRequest().getDestinationPath();
            String consumerGroupName = request.getBody(BFrontend.readConsumerGroupHosts_args.class).getGetHostsRequest().getConsumerGroupName();

            ReadConsumerGroupHostsResult description = new ReadConsumerGroupHostsResult();
            BFrontend.readConsumerGroupHosts_result res = new BFrontend.readConsumerGroupHosts_result();
            ThriftResponse<BFrontend.readConsumerGroupHosts_result> response;

            if (!destinationMap.containsValue(path) || !consumerGroupMap.containsValue(consumerGroupName)) {
                res.setEntityError(new EntityNotExistsError("Entity does not exist"));
                response = new ThriftResponse.Builder<BFrontend.readConsumerGroupHosts_result>(request).setBody(res).setResponseCode(ResponseCode.Error).build();
            } else {
                description.setHostAddresses(hosts);
                res.setSuccess(description);
                response = new ThriftResponse.Builder<BFrontend.readConsumerGroupHosts_result>(request).setBody(res).build();
            }

            return response;
        }
    }

    private static class ReadPublisherOptionsHandler extends ThriftRequestHandler<BFrontend.readPublisherOptions_args, BFrontend.readPublisherOptions_result> {
        List<HostAddress> hosts;

        public void setDestinationHosts(List<HostAddress> newHosts) {
            hosts = newHosts;
        }

        @Override
        public ThriftResponse<BFrontend.readPublisherOptions_result> handleImpl(ThriftRequest<BFrontend.readPublisherOptions_args> request) {
            String path = request.getBody(BFrontend.readPublisherOptions_args.class).getGetPublisherOptionsRequest().getPath();

            ReadPublisherOptionsResult description = new ReadPublisherOptionsResult();
            BFrontend.readPublisherOptions_result res = new BFrontend.readPublisherOptions_result();
            ThriftResponse<BFrontend.readPublisherOptions_result> response;

            if (!destinationMap.containsValue(path)) {
                res.setEntityError(new EntityNotExistsError("Entity does not exist"));
                response = new ThriftResponse.Builder<BFrontend.readPublisherOptions_result>(request).setBody(res).setResponseCode(ResponseCode.Error).build();
            } else {
                description.setHostAddresses(hosts);
                description.setChecksumOption(ChecksumOption.CRC32IEEE);
                res.setSuccess(description);
                response = new ThriftResponse.Builder<BFrontend.readPublisherOptions_result>(request).setBody(res).build();
            }
            return response;
        }
    }

    private static class AckMessagesHandler extends ThriftRequestHandler<BOut.ackMessages_args, BOut.ackMessages_result> {
        @Override
        public ThriftResponse<BOut.ackMessages_result> handleImpl(ThriftRequest<BOut.ackMessages_args> request) {
            BOut.ackMessages_result res = new BOut.ackMessages_result();
            ThriftResponse<BOut.ackMessages_result> response;
            response = new ThriftResponse.Builder<BOut.ackMessages_result>(request).setBody(res).build();
            return response;
        }
    }
}
