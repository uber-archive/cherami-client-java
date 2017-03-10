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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TIOStreamTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An object that takes a list of objects and serializes them into a byte array
 * according to the specified protocol. This implementation is NOT thread-safe
 *
 * @param <T>
 *            Type of the object being serialized.
 */
public class TListSerializer<T extends TBase<?, ?>> {
    private final Logger logger = LoggerFactory.getLogger(TListSerializer.class);
    /**
     * This is the byte array that data is actually serialized into
     */
    private final ByteArrayOutputStream outputStream;
    /**
     * This transport wraps that byte array
     */
    private final TIOStreamTransport transport;
    /**
     * Internal protocol used for serializing objects.
     */
    private final TProtocol protocol;

    public TListSerializer() {
        this(new TBinaryProtocol.Factory());
    }

    /**
     * Constructor for TListSerializer.
     *
     * @param protocolFactory
     *            TProtocolFactory
     */
    public TListSerializer(TProtocolFactory protocolFactory) {
        this.outputStream = new ByteArrayOutputStream();
        this.transport = new TIOStreamTransport(outputStream);
        this.protocol = protocolFactory.getProtocol(transport);
    }

    /**
     * Serialize the List of Thrift objects into a byte array.
     *
     * @param messages The List of Thrift objects to serialize
     * @return Serialized objects in byte[] format
     * @throws TException
     * @throws IOException
     */
    public byte[] serialize(List<T> messages) throws TException, IOException {
        if (messages == null) {
            return null;
        }

        outputStream.reset();
        TList list = new TList(TType.STRING, messages.size());
        try {
            protocol.writeListBegin(list);
        } catch (TException e) {
            logger.error("Error writing list begin: ", e);
            throw e;
        }
        for (TBase<?, ?> msg : messages) {
            try {
                msg.write(protocol);
            } catch (TException e) {
                logger.error("Error writing TSTruct", e);
                throw e;
            }
        }
        try {
            protocol.writeListEnd();
        } catch (TException e) {
            logger.error("Error writing list end: ", e);
            throw e;
        }

        outputStream.flush();
        transport.flush();
        return outputStream.toByteArray();
    }

}
