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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.cherami.ChecksumOption;
import com.uber.cherami.ConsumerMessage;
import com.uber.cherami.PutMessage;

/**
 * Unit tests for generating and verifying the different checksum options
 */
public class ChecksumTest {
    private final Logger logger = LoggerFactory.getLogger(ChecksumTest.class);
    private final PutMessage testPutMessage;
    private final ChecksumValidator checksumValidator;
    private ChecksumWriter checksumWriter;
    private CheramiDeliveryImpl testDelivery;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public ChecksumTest() {
        this.testPutMessage = new PutMessage();
        this.testPutMessage.setData("Hello, world!".getBytes());
        this.checksumValidator = new ChecksumValidator();
    }

    @Before
    public void initializeDelivery() {
        ConsumerMessage consumerMessage = new ConsumerMessage();
        consumerMessage.setPayload(testPutMessage);
        this.testDelivery = new CheramiDeliveryImpl(consumerMessage, null);
    }

    @Test
    public void generateMD5Test() {
        try {
            checksumWriter = ChecksumWriter.create(ChecksumOption.MD5);
        } catch (Exception e) {
            logger.error("MD5 algorithm does not exist", e);
            assert (false);
        }
        byte[] expected = new BigInteger("6cd3556deb0da54bca060b4c39479839", 16).toByteArray();
        checksumWriter.write(testPutMessage);
        assert (Arrays.equals(testPutMessage.getMd5DataChecksum(), expected));
    }

    @Test
    public void md5ValidateSuccessTest() {
        byte[] expected = new BigInteger("6cd3556deb0da54bca060b4c39479839", 16).toByteArray();
        testPutMessage.setMd5DataChecksum(expected);
        try {
            assert (checksumValidator.validate(testDelivery.getMessage()));
        } catch (IOException e) {
            logger.error("MD5 algorithm does not exist", e);
            assert (false);
        }
    }

    @Test
    public void md5ValidateFailureTest() {
        try {
            byte[] checksum = "1234567890ABC".getBytes(UTF_8);
            testPutMessage.setMd5DataChecksum(checksum);
            assert (checksumValidator.validate(testDelivery.getMessage()) == false);
        } catch (IOException e) {
            logger.error("MD5 algorithm does not exist", e);
            assert (false);
        }
    }

    @Test
    public void generateCRC32IEEETest() {
        try {
            checksumWriter = ChecksumWriter.create(ChecksumOption.CRC32IEEE);
        } catch (Exception e) {
            logger.error("Algorithm does not exist");
            assert (false);
        }
        long expected = 3957769958L;
        checksumWriter.write(testPutMessage);
        assert (testPutMessage.getCrc32IEEEDataChecksum() == expected);
    }

    @Test
    public void crc32IEEEValidateSuccessTest() {
        long expected = 3957769958L;
        testPutMessage.setCrc32IEEEDataChecksum(expected);
        try {
            assert (checksumValidator.validate(testDelivery.getMessage()));
        } catch (IOException e) {
            logger.error("Algorithm does not exist", e);
            assert (false);
        }
    }

    @Test
    public void crc32IEEEValidateFailureTest() {
        try {
            long checksum = 100000000L;
            testPutMessage.setCrc32IEEEDataChecksum(checksum);

            assert (checksumValidator.validate(testDelivery.getMessage()) == false);
        } catch (IOException e) {
            logger.error("Algorithm does not exist", e);
            assert (false);
        }

        logger.info("ChecksumTests: PASSED: crc32IEEEValidateFailureTest");
    }

    @Test
    public void multipleChecksumTest() {
        try {
            checksumWriter = ChecksumWriter.create(ChecksumOption.CRC32IEEE);
        } catch (Exception e) {
            logger.error("Algorithm does not exist", e);
            assert (false);
        }
        checksumWriter.write(testPutMessage);
        byte[] incorrectmd5 = "12345".getBytes(UTF_8);
        testPutMessage.setMd5DataChecksum(incorrectmd5);

        try {
            // checksum verification is done in order
            // since crc32IEEE checksum verification is done first, other invalid checksums won't matter
            assert (checksumValidator.validate(testDelivery.getMessage()));
        } catch (IOException e) {
            logger.error("Algorithm does not exist", e);
            assert (false);
        }
        logger.info("ChecksumTests: PASSED: multipleChecksumTest");
    }

    @Test
    public void noPayloadTest() throws IOException {
        assert (!checksumValidator.validate(new ConsumerMessage()));
    }
}
