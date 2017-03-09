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
package com.uber.cherami.example;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.uber.cherami.ChecksumOption;
import com.uber.cherami.ConsumerGroupDescription;
import com.uber.cherami.CreateConsumerGroupRequest;
import com.uber.cherami.CreateDestinationRequest;
import com.uber.cherami.DeleteConsumerGroupRequest;
import com.uber.cherami.DeleteDestinationRequest;
import com.uber.cherami.DestinationDescription;
import com.uber.cherami.DestinationType;
import com.uber.cherami.client.CheramiClient;
import com.uber.cherami.client.CheramiConsumer;
import com.uber.cherami.client.CheramiDelivery;
import com.uber.cherami.client.CheramiPublisher;
import com.uber.cherami.client.ClientOptions;
import com.uber.cherami.client.CreateConsumerRequest;
import com.uber.cherami.client.CreatePublisherRequest;
import com.uber.cherami.client.PublisherMessage;
import com.uber.cherami.client.SendReceipt;
import com.uber.cherami.client.SendReceipt.ReceiptStatus;

/**
 * Example demonstrating publishing / consuming messages from cherami.
 */
public class Example {

    private final Stats stats;
    private final Config config;
    private final CheramiClient client;
    private final AtomicBoolean isClosed;

    public Example(Config config) {
        this.config = config;
        this.client = buildClient(config);
        this.stats = new Stats();
        this.isClosed = new AtomicBoolean();
    }

    private CheramiClient buildClient(Config config) {
        try {
            ClientOptions options = new ClientOptions.Builder().setDeploymentStr("staging").build();
            if (!config.ip.isEmpty()) {
                // production must also always use service discovery
                return new CheramiClient.Builder(config.ip, config.port).setClientOptions(options).build();
            }
            // production must also set the metricsClient option
            return new CheramiClient.Builder().setClientOptions(options).build();
        } catch (Exception e) {
            System.out.println("Failed to create CheramiClient:" + e);
            throw new RuntimeException(e);
        }
    }

    private void doSetup() {
        try {
            CreateDestinationRequest dstRequest = new CreateDestinationRequest();
            dstRequest.setPath(config.destinationPath);
            dstRequest.setType(DestinationType.PLAIN);
            dstRequest.setUnconsumedMessagesRetention(7200);
            dstRequest.setConsumedMessagesRetention(3600);
            dstRequest.setOwnerEmail("cherami-client-example@uber.com");
            dstRequest.setChecksumOption(ChecksumOption.CRC32IEEE);
            DestinationDescription producer = client.createDestination(dstRequest);
            System.out.println("Created Destination:\n" + producer);
            // Create a ConsumerGroup
            CreateConsumerGroupRequest cgRequest = new CreateConsumerGroupRequest();
            cgRequest.setDestinationPath(config.destinationPath);
            cgRequest.setConsumerGroupName(config.consumerName);
            cgRequest.setOwnerEmail("cherami-client-example@uber.com");
            cgRequest.setMaxDeliveryCount(3);
            cgRequest.setSkipOlderMessagesInSeconds(3600);
            cgRequest.setLockTimeoutInSeconds(60);
            cgRequest.setStartFrom(System.currentTimeMillis());
            ConsumerGroupDescription consumerGroup = client.createConsumerGroup(cgRequest);
            System.out.println("Created Consumer Group:\n" + consumerGroup);
        } catch (Exception e) {
            System.out.println("Error setting up destination and consumer group:" + e);
            throw new RuntimeException(e);
        }
    }

    private void doTearDown() {
        if (isClosed.getAndSet(true)) {
            return;
        }
        DeleteConsumerGroupRequest cgRequest = new DeleteConsumerGroupRequest();
        cgRequest.setDestinationPath(config.destinationPath);
        cgRequest.setConsumerGroupName(config.consumerName);
        try {
            client.deleteConsumerGroup(cgRequest);
            System.out.println("Deleted ConsumerGroup " + config.consumerName);
        } catch (Exception e) {
            System.out.println("Error deleting consumer group:" + e);
        }
        DeleteDestinationRequest dstRequest = new DeleteDestinationRequest();
        dstRequest.setPath(config.destinationPath);
        try {
            client.deleteDestination(dstRequest);
            System.out.println("Deleted Destination " + config.destinationPath);
        } catch (Exception e) {
            System.out.println("Error deleting destination:" + e);
        }
        try {
            client.close();
        } catch (IOException e) {
            System.out.println("Error closing CheramiClient:" + e);
        }
    }

    private static class InFlightMsgState {

        public int attempts;
        public final long sendTime;
        public final Future<SendReceipt> future;

        public final long id;

        public InFlightMsgState(long id, Future<SendReceipt> future, int attempts) {
            this.id = id;
            this.future = future;
            this.attempts = attempts;
            this.sendTime = System.currentTimeMillis();
        }
    }

    private PublisherMessage createPubMessage(long id, Random random) {
        byte[] payload = new byte[config.messageSize];
        random.nextBytes(payload);
        AppData data = new AppData(id, payload);
        return new PublisherMessage(data.serialize());
    }

    public void run() throws Exception {

        doSetup();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                doTearDown();
            }
        });

        CheramiPublisher publisher = null;
        CheramiConsumer consumer = null;

        try {

            CreatePublisherRequest.Builder builder = new CreatePublisherRequest.Builder(config.destinationPath);
            publisher = client.createPublisher(builder.build());
            publisher.open();

            final int maxInFlight = 4 * 1024;
            final AtomicInteger msgCounter = new AtomicInteger(1);

            int remaining = config.numMessagesToSend;
            Queue<InFlightMsgState> inflight = new LinkedList<>();

            Random rand = new Random(System.currentTimeMillis());

            long startTime = System.currentTimeMillis();

            while (inflight.size() > 0 || remaining > 0) {

                int attempts = 0;
                long retryMsgId = 0;

                if (inflight.size() >= maxInFlight || remaining == 0) {
                    // Can't send anymore until we receive
                    // the acks for the previously sent
                    // messages. Read an ack and for every
                    // ack, send another message out
                    InFlightMsgState state = inflight.poll();
                    SendReceipt receipt = state.future.get();
                    stats.writeLatency.add(System.currentTimeMillis() - state.sendTime);
                    if (receipt.getStatus() != ReceiptStatus.OK) {
                        stats.messagesOutErrCount.incrementAndGet();
                        if (state.attempts >= 16) {
                            System.out.println("Max attempts exceeded at sending a message");
                            return;
                        }
                        if (receipt.getStatus() == ReceiptStatus.ERR_THROTTLED) {
                            // ideally, exponential backoff and retry
                            Thread.sleep(1);
                            stats.messagesOutThrottledCount.incrementAndGet();
                        }

                        attempts = state.attempts;
                        retryMsgId = state.id;
                    }
                }

                if (retryMsgId == 0 && remaining == 0) {
                    continue; // no new or old messages to re-send
                }

                long msgId = retryMsgId != 0 ? retryMsgId : msgCounter.incrementAndGet();
                PublisherMessage message = createPubMessage(msgId, rand);
                InFlightMsgState state = new InFlightMsgState(msgId, publisher.writeAsync(message), attempts + 1);
                inflight.add(state);
                stats.messagesOutCount.incrementAndGet();
                stats.bytesOutCount.addAndGet(message.getData().length);
                remaining = (retryMsgId == 0) ? (remaining - 1) : remaining;
            }

            stats.totalPublishLatency.add(System.currentTimeMillis() - startTime);

            CreateConsumerRequest request = new CreateConsumerRequest.Builder(config.destinationPath,
                    config.consumerName).setPrefetchCount(maxInFlight).build();
            consumer = client.createConsumer(request);
            consumer.open();

            Set<Long> receivedIds = new HashSet<>(config.numMessagesToSend);

            while (receivedIds.size() < config.maxNumberToReceive) {
                startTime = System.currentTimeMillis();
                CheramiDelivery delivery = consumer.read();
                stats.readLatency.add(System.currentTimeMillis() - startTime);
                AppData appData = AppData.deserialize(delivery.getMessage().getPayload().getData());
                if (receivedIds.contains(appData.id)) {
                    stats.messagesInDupCount.incrementAndGet();
                } else {
                    receivedIds.add(appData.id);
                }
                delivery.ack();
                stats.messagesInCount.incrementAndGet();
                stats.bytesInCount.addAndGet(delivery.getMessage().getPayload().getData().length);
            }

        } catch (Throwable e) {
            System.out.println("Caught unexpected exception:" + e);
        } finally {
            if (publisher != null) {
                publisher.close();
            }
            if (consumer != null) {
                consumer.close();
            }
            doTearDown();
            stats.print();
        }
    }

    public static void main(String[] args) throws Exception {
        final Config config = Config.parse(args);
        new Example(config).run();
    }
}
