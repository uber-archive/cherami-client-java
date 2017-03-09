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

import com.uber.cherami.ChecksumOption;
import com.uber.cherami.PutMessage;

/**
 * Contract for any implementation that can stamp a checkum onto a PutMessage.
 *
 * @author venkat
 */
public abstract class ChecksumWriter {
    /**
     * Computes and writes the checksum.
     *
     * @param message
     *            PutMessage the message for which checksum needs to be
     *            computed.
     */
    public abstract void write(PutMessage message);

    /**
     * Creates a CheckumWriter for the given checkum type.
     *
     * @param option
     *            ChecksumOption identifying the type of checksum.
     * @return ChecksumWriter for stamping the checksum.
     */
    public static ChecksumWriter create(ChecksumOption option) {
        switch (option) {
        case CRC32IEEE:
            return new CRC32Writer();
        case MD5:
            return new MD5Writer();
        default:
            return new NoopWriter();
        }
    }

    /**
     * ChecksumWriter that does nothing. Serves as the default.
     *
     * @author venkat
     */
    private static class NoopWriter extends ChecksumWriter {
        @Override
        public void write(PutMessage message) {
            return;
        }
    }
}
