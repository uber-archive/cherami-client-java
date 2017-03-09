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
import java.util.List;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An object that takes a byte array of serialized messages and deserializes them into a list of Thrift objects
 * Note that this implementation is NOT thread-safe
 */
public class TListDeserializer<T extends TBase<?, ?>> {
    private final Logger logger = LoggerFactory.getLogger(TListDeserializer.class);
    private final TMemoryInputTransport transport;
    private final TProtocol protocol;

    public TListDeserializer() {
        this(new TBinaryProtocol.Factory());
    }

    public TListDeserializer(TProtocolFactory protocolFactory) {
        this.transport = new TMemoryInputTransport();
        this.protocol = protocolFactory.getProtocol(transport);
    }

    /**
     * Deserialize a list of Thrift objects from a byte array
     *
     * @param classType The class type of the Thrift Objects
     * @param msgs      The byte array to read from
     * @return A list of deserialized Thrift objects
     * @throws TException
     */
    public List<T> deserialize(Class<T> classType, byte[] msgs) throws TException {
        List<T> messages = new ArrayList<>();

        transport.reset(msgs, 0, msgs.length);

        TList list;
        try {
            list = protocol.readListBegin();
        } catch (TException e) {
            logger.error("Error reading list begin", e);
            throw e;
        }

        for (int i = 0; i < list.size; i++) {
            try {
                T command = classType.newInstance();
                command.read(protocol);
                messages.add(command);
            } catch (TException e) {
                logger.error("Error reading TStruct", e);
                throw e;
            } catch (InstantiationException | IllegalAccessException e) {
                logger.error("Error Creating new instance", e);
            }
        }
        try {
            protocol.readListEnd();
        } catch (TException e) {
            logger.error("Error reading list end", e);
            throw e;
        }
        return messages;
    }
}
