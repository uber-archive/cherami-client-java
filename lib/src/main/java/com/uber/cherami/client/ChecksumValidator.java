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

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.zip.CRC32;

import com.uber.cherami.ConsumerMessage;
import com.uber.cherami.PutMessage;

/**
 * ChecksumValidator validates a given ConsumerMessage
 * Note: This class is NOT thread safe
 */
public class ChecksumValidator {
    private MessageDigest md;
    private CRC32 crc32;

    private CRC32 getCrc32() {
        if (crc32 == null) {
            crc32 = new CRC32();
        }
        return crc32;
    }

    private MessageDigest getMessageDigest() throws IOException {
        if (md == null) {
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IOException(e);
            }
        }
        return md;
    }

    /**
     * Validates whether or not the received checksum is valid. If no checksum field is set, the method will return TRUE
     * If multiple checksum fields are set, the checksums are validated in order of CRC32IEEE, CRC32C, then MD5.
     *
     * @param message The ConsumerMessage that was received from Cherami
     * @return TRUE if the received checksum is valid, else FALSE
     * @throws IOException
     */
    public boolean validate(ConsumerMessage message) throws IOException {
        if (!message.isSetPayload()) {
            return false;
        }
        PutMessage payload = message.getPayload();

        if (payload.isSetCrc32IEEEDataChecksum()) {
            CRC32 crc32 = getCrc32();
            long received = payload.getCrc32IEEEDataChecksum();
            crc32.update(payload.getData());
            long expected = crc32.getValue();
            crc32.reset();
            return received == expected;
        } else if (payload.isSetMd5DataChecksum()) {
            MessageDigest md = getMessageDigest();
            byte[] received = payload.getMd5DataChecksum();
            byte[] expected = md.digest(payload.getData());
            md.reset();
            return Arrays.equals(received, expected);
        }
        // no known checksum provided, just pass
        return true;
    }
}
