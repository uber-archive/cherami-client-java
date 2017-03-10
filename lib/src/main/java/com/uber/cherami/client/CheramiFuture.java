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

import java.sql.Timestamp;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cherami implementation of Future interface.
 *
 * @param <T>
 *            The result type held by this future.
 */
public class CheramiFuture<T> implements Future<T> {

    private final Timestamp sentTime;
    private final CountDownLatch isDone = new CountDownLatch(1);
    private final AtomicReference<Exception> error = new AtomicReference<>();
    private final AtomicReference<T> reply = new AtomicReference<>();

    public CheramiFuture() {
        this.sentTime = new Timestamp(System.currentTimeMillis());
    }

    public void setReply(T result) {
        this.reply.set(result);
        isDone.countDown();
    }

    public void setError(Exception e) {
        this.error.set(e);
        isDone.countDown();
    }

    /**
     * @return Time when the future is created.
     */
    public Timestamp getSentTime() {
        return sentTime;
    }

    /**
     * Cancel is unsupported. Calling cancel will always return false.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    /**
     * @return Always returns false.
     */
    @Override
    public boolean isCancelled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isDone() {
        return isDone.getCount() == 0;
    }

    /** {@inheritDoc} */
    @Override
    public T get() throws InterruptedException, ExecutionException {
        isDone.await();
        if (error.get() != null) {
            throw new ExecutionException(error.get());
        }
        return reply.get();
    }

    /** {@inheritDoc} */
    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (!isDone.await(timeout, unit)) {
            throw new TimeoutException();
        }
        return this.get();
    }
}
