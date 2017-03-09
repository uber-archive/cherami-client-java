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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.cherami.PutMessage;

/**
 * Unit tests for TListSerializer and TListDeserializer
 */
public class CheramiSerializationTest {
    private static final Logger logger = LoggerFactory.getLogger(CheramiSerializationTest.class);
    private static TListSerializer<PutMessage> serializer;
    private static TListDeserializer<PutMessage> deserializer;

    @BeforeClass
    public static void setup() {
        serializer = new TListSerializer<PutMessage>(new TBinaryProtocol.Factory());
        deserializer = new TListDeserializer<PutMessage>(new TBinaryProtocol.Factory());
    }

    @Test
    public void tListSerializerTest() {
        List<PutMessage> list = new ArrayList<>();
        Random r = new Random();
        byte[] data = new byte[1024];
        r.nextBytes(data);
        PutMessage msg = new PutMessage();
        msg.setData(data);
        msg.setId("Msg1");
        PutMessage msg1 = new PutMessage();
        msg1.setData(data);
        msg.setId("Msg2");

        list.add(msg);
        list.add(msg1);

        try {
            byte[] serialized = serializer.serialize(list);
            assert (serialized.length > 0);
        } catch (Exception e) {
            logger.error("Error in serializing list", e);
            assert (false);
        }
        logger.info("CheramiSerializationTest: PASSED: tListSerializerTest");
    }

    @Test
    public void tListDeserializerTest() {
        List<PutMessage> list = new ArrayList<>();
        Random r = new Random();
        byte[] data = new byte[1024];
        r.nextBytes(data);
        PutMessage msg = new PutMessage();
        msg.setData(data);
        msg.setId("Msg1");

        byte[] data1 = new byte[1024];
        r.nextBytes(data1);

        PutMessage msg1 = new PutMessage();
        msg1.setData(data1);
        msg1.setId("Msg2");

        list.add(msg);
        list.add(msg1);

        byte[] serialized = new byte[0];
        try {
            serialized = serializer.serialize(list);
            assert (serialized.length > 0);
        } catch (Exception e) {
            logger.error("Error in serializing list", e);
            assert (false);
        }

        try {
            //Map ID to data
            HashMap<String, byte[]> dataMap = new HashMap<>();
            dataMap.put("Msg1", data);
            dataMap.put("Msg2", data1);

            List<PutMessage> messages = deserializer.deserialize(PutMessage.class, serialized);
            ArrayList<String> ids = new ArrayList<>();
            ids.add("Msg1");
            ids.add("Msg2");
            assert (messages.size() == 2);
            assert (ids.remove(messages.get(0).getId()));
            assert (Arrays.equals(dataMap.get(messages.get(0).getId()), messages.get(0).getData()));
            assert (ids.remove(messages.get(1).getId()));
            assert (Arrays.equals(dataMap.get(messages.get(1).getId()), messages.get(1).getData()));
            assert (ids.isEmpty());
        } catch (TException e) {
            logger.error("Error in deserializing list", e);
            assert (false);
        }
        logger.info("CheramiSerializationTest: PASSED: tListDeserializerTest");
    }
}
