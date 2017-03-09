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

/**
 * Value type that holds the publisher options.
 *
 * @author venkat
 */
public class PublisherOptions {

    public final boolean keepAlive;
    public final long sndBufSizeBytes;
    public final long writeTimeoutMillis;
    public final int inFlightMsgsPerConn;


    /**
     * Creates and returns a PublisherOptions object.
     *
     * @param sndBufSizeBytes
     *            long representing the send buffer size, in bytes.
     * @param inFlighMsgsPerConn
     *            int representing the max inflight messages per connection.
     * @param writeTimeoutMillis
     *            long representing the message publish timeout.
     */
    public PublisherOptions(long sndBufSizeBytes, int inFlighMsgsPerConn, long writeTimeoutMillis) {
        this.sndBufSizeBytes = sndBufSizeBytes;
        this.writeTimeoutMillis = writeTimeoutMillis;
        this.inFlightMsgsPerConn = inFlighMsgsPerConn;
        this.keepAlive = true;
    }
}
