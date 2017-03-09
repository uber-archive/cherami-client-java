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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MessageQueue that is capacity constrained by number of bytes.
 *
 * This class is a wrapper around LinkedBlockingQueue that enforces capacity by
 * bytes rather than number of items. This queue serves as the send buffer for
 * messages that need to be published.
 *
 * The capacity guarantees provided by this class is not designed to be
 * accurate. i.e. This implementation guarantees that if the capacity is x, then
 * no more items can be enqueued after the size reaches x + delta, where delta
 * is very small.
 *
 * @author venkat
 */
public class SendBufferQueue {

    private final long capacityBytes;
    private final AtomicLong sizeBytes;
    private final LinkedBlockingQueue<PutMessageRequest> blockingQueue;

    /**
     * Constructs and returns a SendBufferQueue of requested size.
     *
     * @param capacityBytes
     *            Max number of bytes that this queue can hold.
     */
    public SendBufferQueue(long capacityBytes) {
        super();
        this.capacityBytes = capacityBytes;
        this.sizeBytes = new AtomicLong(0);
        this.blockingQueue = new LinkedBlockingQueue<>();
    }

    /**
     * Attempts to enqueue the given message onto the queue if the capacity
     * constraint will not be violated.
     *
     * @param msg
     *            PutMessageRequest to be enqueued.
     * @return True on success, false if the buffer is full.
     */
    public boolean offer(PutMessageRequest msg) {
        long nBytes = msg.getMessage().getData().length;
        if (sizeBytes.get() + nBytes >= capacityBytes) {
            return false;
        }
        sizeBytes.addAndGet(nBytes);
        this.blockingQueue.add(msg);
        return true;
    }

    /**
     * Removes and returns the head of the queue.
     *
     * @return PutMessageRequest on success, null if the queue is empty.
     */
    public PutMessageRequest poll() {
        PutMessageRequest msg = blockingQueue.poll();
        if (msg == null) {
            return null;
        }
        long delta = -1L * msg.getMessage().getData().length;
        sizeBytes.addAndGet(delta);
        return msg;
    }

    /**
     * @return True if the queue is empty.
     */
    public boolean isEmpty() {
        return blockingQueue.isEmpty();
    }

    /**
     * @return int representing the number of items in the queue.
     */
    public int size() {
        return blockingQueue.size();
    }

    /**
     * @return long representing the size of the queue in bytes.
     */
    public long bytes() {
        return sizeBytes.get();
    }
}
