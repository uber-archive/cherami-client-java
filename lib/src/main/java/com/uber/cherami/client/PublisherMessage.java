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
 * PublisherMessage is an object that wraps the message payload and the number of seconds to wait before delivery
 */
public class PublisherMessage {
    private final byte[] data;
    /**
     * The number of seconds to wait before publishing
     */
    private final int delaySeconds;

    /**
     * Create a PublisherMessage which contains the data to publish immediately
     *
     * @param data The data to publish to Cherami
     */
    public PublisherMessage(byte[] data) {
        this.data = data;
        this.delaySeconds = 0;
    }

    /**
     * Create a PublisherMessage which contains the data to publish after delaySeconds
     *
     * @param data         The data to publish to Cherami
     * @param delaySeconds The seconds to wait before publishing.
     */
    public PublisherMessage(byte[] data, int delaySeconds) {
        this.data = data;
        this.delaySeconds = delaySeconds;
    }

    public byte[] getData() {
        return data;
    }

    public int getDelaySeconds() {
        return delaySeconds;
    }
}
