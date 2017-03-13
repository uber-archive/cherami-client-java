package com.uber.cherami.example;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.uber.cherami.client.CheramiConsumer;
import com.uber.cherami.client.CheramiDelivery;
import com.uber.cherami.client.CheramiPublisher;
import com.uber.cherami.client.CreateConsumerRequest;
import com.uber.cherami.client.CreatePublisherRequest;
import com.uber.cherami.client.PublisherMessage;
import com.uber.cherami.client.SendReceipt;
import com.uber.cherami.client.SendReceipt.ReceiptStatus;

/**
 * Demonstrates publishing / consuming from cherami using the sync blocking
 * api..
 *
 * @author venkat
 */
public class Sync {

    /**
     * Publisher publishes messages to cherami.
     *
     * @author venkat
     */
    public static class Publisher implements Runnable, Daemon {

        private final String name;
        private final Context context;
        private final AtomicLong msgCounter;

        private final Random random = new Random();
        private final CountDownLatch quitter = new CountDownLatch(1);
        private final CountDownLatch stopped = new CountDownLatch(1);

        /**
         * Constructs and returns a Publisher object.
         *
         * @param name
         *            String representing the unique name of the publisher.
         * @param context
         *            Context object.
         * @param msgCounter
         *            AtomicLong to be used for generation of message
         *            identifiers.
         */
        public Publisher(String name, Context context, AtomicLong msgCounter) {
            this.name = name;
            this.context = context;
            this.msgCounter = msgCounter;
        }

        @Override
        public void start() {
            new Thread(this).start();
        }

        @Override
        public void stop() {
            quitter.countDown();
            try {
                if (!stopped.await(1, TimeUnit.SECONDS)) {
                    System.out.println(name + ": shutdown timed out");
                }
            } catch (InterruptedException e) {
                return;
            }
        }

        @Override
        public void run() {

            CheramiPublisher publisher = null;
            try {

                System.out.println(name + " started");

                Config config = context.config;

                CreatePublisherRequest.Builder builder = new CreatePublisherRequest.Builder(config.destinationPath);
                publisher = context.client.createPublisher(builder.build());
                publisher.open();

                long remaining = config.nMessagesToSend;
                Stats.Profiler writeLatencyProfiler = new Stats.Profiler();

                while (remaining > 0 && quitter.getCount() > 0) {

                    long id = msgCounter.incrementAndGet();
                    PublisherMessage message = createMessage(id);

                    writeLatencyProfiler.start();
                    SendReceipt receipt = null;
                    do {
                        receipt = publisher.write(message);
                        switch (receipt.getStatus()) {
                        case OK:
                            remaining--;
                            context.stats.writeLatency.add(writeLatencyProfiler.elapsed());
                            context.stats.messagesOutCount.incrementAndGet();
                            context.stats.bytesOutCount.addAndGet(message.getData().length);
                            break;
                        case ERR_THROTTLED:
                            context.stats.messagesOutThrottledCount.incrementAndGet();
                            context.stats.messagesOutErrCount.incrementAndGet();
                            // ideally, exponential backoff here
                            sleep(100);
                            break;
                        default:
                            context.stats.messagesOutErrCount.incrementAndGet();
                            break;
                        }
                    } while (receipt.getStatus() != ReceiptStatus.OK && quitter.getCount() > 0);
                }

            } catch (Throwable e) {
                System.out.println("Publisher caught unexpected exception: " + e);
            } finally {
                if (publisher != null) {
                    publisher.close();
                }
                System.out.println(name + " closed");
                stopped.countDown();
            }
        }

        private void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                return;
            }
        }

        private PublisherMessage createMessage(long id) {
            byte[] payload = new byte[context.config.messageSize];
            random.nextBytes(payload);
            AppData data = new AppData(id, payload);
            return new PublisherMessage(data.serialize());
        }
    }

    /**
     * Consumes messages from cherami using the synchronous api.
     *
     * @author venkat
     */
    public static class Consumer implements Runnable, Daemon {

        /**
         * Number of messages that the client library will prefetch into its
         * receive buffer. This must be a function of msgProcessingTime,
         * redeliveryTimeout and RTT to server. Setting this incorrectly will
         * result in un-necessary redeliveries and in-turn, duplicates. For long
         * running tasks (processingTimes in minutes), prefetch=1 is the desired
         * value.
         */
        private static final int PREFETCH_COUNT = 256;

        private final String name;
        private final Context context;
        private final Set<Long> receivedMsgIdsUniq;

        private final CountDownLatch quitter = new CountDownLatch(1);
        private final CountDownLatch stopped = new CountDownLatch(1);

        private CheramiConsumer consumer;

        /**
         * Constructs and returns a Consumer object.
         *
         * @param name
         *            String representing the unique name of the consumer.
         * @param context
         *            Context object.
         * @param receivedMsgIdsUniq
         *            Set of unique message ids received so far.
         */
        public Consumer(String name, Context context, Set<Long> receivedMsgIdsUniq) {
            this.name = name;
            this.context = context;
            this.receivedMsgIdsUniq = receivedMsgIdsUniq;
        }

        @Override
        public void start() {
            new Thread(this).start();
        }

        @Override
        public void stop() {
            quitter.countDown();
            if (consumer != null) {
                // force the thread to wake up from read()
                consumer.close();
            }
            try {
                if (!stopped.await(1, TimeUnit.SECONDS)) {
                    System.out.println(name + ": shutdown timed out");
                }
            } catch (InterruptedException e) {
                return;
            }
        }

        @Override
        public void run() {
            try {

                System.out.println(name + " started");

                Config config = context.config;

                CreateConsumerRequest.Builder builder = new CreateConsumerRequest.Builder(config.destinationPath,
                        config.consumergroupName);
                CreateConsumerRequest request = builder.setPrefetchCount(PREFETCH_COUNT).build();
                this.consumer = context.client.createConsumer(request);
                consumer.open();

                Stats.Profiler readLatencyProfiler = new Stats.Profiler();

                while (quitter.getCount() > 0) {

                    CheramiDelivery delivery = null;
                    try {
                        readLatencyProfiler.start();
                        delivery = consumer.read();
                        context.stats.readLatency.add(readLatencyProfiler.elapsed());
                    } catch (InterruptedException e) {
                        continue;
                    }

                    byte[] data = delivery.getMessage().getPayload().getData();
                    AppData appData = AppData.deserialize(data);
                    if (receivedMsgIdsUniq.contains(appData.id)) {
                        context.stats.messagesInDupCount.incrementAndGet();
                    } else {
                        receivedMsgIdsUniq.add(appData.id);
                    }

                    delivery.ack();

                    context.stats.messagesInCount.incrementAndGet();
                    context.stats.bytesInCount.addAndGet(data.length);
                }
            } catch (Throwable e) {
                System.out.println(name + " caught unexpected exception:" + e);
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
                System.out.println(name + " stopped");
                stopped.countDown();
            }
        }
    }
}
