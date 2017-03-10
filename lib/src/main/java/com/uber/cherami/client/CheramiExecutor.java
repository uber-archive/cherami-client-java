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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A task executor that receives messages and passes them into the MessageHandler that is implemented by the Consumer.
 * If the handler succeeds, the message will be ACKed. Otherwise, it will be NACKed
 */
public class CheramiExecutor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CheramiExecutor.class);

    private final Executor executor;
    private final CheramiConsumerImpl consumer;
    private final MessageHandler handler;
    private final CountDownLatch quitter;
    private final CountDownLatch stoppedLatch;

    /**
     * Creates a CheramiExecutor object.
     *
     * @param executor
     *            The Executor object used to run this executor
     * @param consumer
     *            The CheramiConsumer object to use to consume messages
     * @param handler
     *            The MessageHandler implemented by the Consumer, used to
     *            deserialize and process messages
     */
    public CheramiExecutor(Executor executor, CheramiConsumer consumer, MessageHandler handler) {
        this.executor = executor;
        this.consumer = (CheramiConsumerImpl) consumer;
        this.handler = handler;
        this.quitter = new CountDownLatch(1);
        this.stoppedLatch = new CountDownLatch(1);
    }

    /**
     * Stops the executor.
     *
     * @param awaitTime
     *            Amount of time to wait for the underlying threads to stop.
     * @param timeUnit
     *            TimeUnit for awaitTime.
     */
    public void stop(long awaitTime, TimeUnit timeUnit) {
        try {
            quitter.countDown();
            stoppedLatch.await(awaitTime, timeUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void sleep(long millis) {
        try {
            quitter.await(millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return;
        }
    }

    private CheramiDelivery awaitFuture(Future<CheramiDelivery> future) throws ExecutionException {
        CheramiDelivery result = null;
        while (result == null && quitter.getCount() > 0) {
            try {
                result = future.get(1, TimeUnit.SECONDS);
            } catch (TimeoutException | InterruptedException e) {
                continue;
            }
        }
        return result;
    }

    @Override
    public void run() {
        CheramiDelivery delivery = null;
        try {
            while (quitter.getCount() > 0 && consumer.isOpen()) {
                try {
                    if (delivery == null) {
                        Future<CheramiDelivery> future = consumer.readAsync();
                        delivery = awaitFuture(future);
                        if (delivery == null) {
                            break;
                        }
                    }
                    executor.execute(new MessageHandlerRunnable(delivery, handler));
                } catch (RejectedExecutionException e) {
                    // Backoff and redeliver on a rejected execution exception
                    logger.warn("Executor ran out of worker threads");
                    sleep(100);
                    continue;
                } catch (Exception e) {
                    logger.error("Caught unexpected error.", e);
                }
                delivery = null;
            }
        } catch (Throwable e) {
            logger.error("CheramiExecutor caught expected exception, exiting.", e);
        } finally {
            stoppedLatch.countDown();
        }
    }

    private class MessageHandlerRunnable implements Runnable {
        private final CheramiDelivery delivery;
        private final MessageHandler handler;

        MessageHandlerRunnable(CheramiDelivery delivery, MessageHandler handler) {
            this.delivery = delivery;
            this.handler = handler;
        }

        @Override
        public void run() {
            try {
                if (handler.handle(delivery.getMessage())) {
                    delivery.ack();
                    return;
                }
            } catch (Throwable e) {
                logger.error("Error from MessageHandler for message with delivery token {}",
                        delivery.getDeliveryToken(), e);
            }
            delivery.nack();
        }
    }
}
