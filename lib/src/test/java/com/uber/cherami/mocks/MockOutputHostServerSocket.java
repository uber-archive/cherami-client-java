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
package com.uber.cherami.mocks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.cherami.ConsumerMessage;
import com.uber.cherami.ControlFlow;
import com.uber.cherami.OutputHostCommand;
import com.uber.cherami.OutputHostCommandType;
import com.uber.cherami.PutMessage;
import com.uber.cherami.client.TListDeserializer;
import com.uber.cherami.client.TListSerializer;

/**
 * A mock websocket server for the OutputHost.
 * On connection, create two PutMessage objects to represent published messages
 */
@WebSocket
public class MockOutputHostServerSocket {
    private static final Logger logger = LoggerFactory.getLogger(MockOutputHostServerSocket.class);
    private final ArrayDeque<PutMessage> messages = new ArrayDeque<>();
    private int totalCredits = -9;

    @OnWebSocketMessage
    public void processUpload(Session session, byte[] input, int offest, int length) throws IOException, TException {
        TListDeserializer<ControlFlow> deserializer = new TListDeserializer<>(new TBinaryProtocol.Factory());

        List<ControlFlow> controlFlows = deserializer.deserialize(ControlFlow.class, input);
        for (ControlFlow flow : controlFlows) {
            totalCredits += flow.getCredits();

            TListSerializer<OutputHostCommand> serializer = new TListSerializer<>(new TBinaryProtocol.Factory());
            List<OutputHostCommand> list = new ArrayList<>();
            while (totalCredits > 0 && !messages.isEmpty()) {
                ConsumerMessage msg = new ConsumerMessage();
                PutMessage putMessage = messages.poll();
                msg.setAckId(putMessage.getId());
                msg.setPayload(putMessage);
                msg.setEnqueueTimeUtc(System.currentTimeMillis());

                OutputHostCommand command = new OutputHostCommand();
                command.setMessage(msg);
                command.setType(OutputHostCommandType.MESSAGE);

                list.add(command);
                totalCredits--;
            }
            if (!list.isEmpty()) {
                byte[] payload = serializer.serialize(list);
                session.getRemote().sendBytes(ByteBuffer.wrap(payload));
            }
        }
    }

    @OnWebSocketConnect
    public void onConnect(Session session) throws IOException, TException {
        Random rand = new Random();
        byte[] data = new byte[1024];
        rand.nextBytes(data);
        for (int i = 0; i < 200; i++) {
            PutMessage putMessage = new PutMessage();
            putMessage.setId(Integer.toString(i));
            putMessage.setData(data);
            messages.add(putMessage);
        }

        logger.debug("{} connected", session.getRemoteAddress().getHostName());
    }

    @OnWebSocketClose
    public void onClose(Session session, int status, String reason) {
        logger.debug("{} closed. Reason: {}", session.getRemoteAddress().getHostName(), reason);
    }
}
