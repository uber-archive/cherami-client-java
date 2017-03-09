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

import com.uber.cherami.InputHostCommand;
import com.uber.cherami.InputHostCommandType;
import com.uber.cherami.PutMessage;
import com.uber.cherami.PutMessageAck;
import com.uber.cherami.Status;
import com.uber.cherami.client.TListDeserializer;
import com.uber.cherami.client.TListSerializer;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A mock echo server that sends back the message that was received
 */
@WebSocket
public class MockServerSocket {
    private static final Logger logger = LoggerFactory.getLogger(MockServerSocket.class);

    @OnWebSocketMessage
    public void processUpload(Session session, byte[] input, int offest, int length) throws IOException, TException, InterruptedException {
        TListDeserializer deserializer = new TListDeserializer(new TBinaryProtocol.Factory());

        List<PutMessage> messages = deserializer.deserialize(PutMessage.class, input);

        for (PutMessage message : messages) {
            String msg = new String(message.getData(), UTF_8);
            PutMessageAck ack = new PutMessageAck();
            if (msg.contains("world")) {
                ack.setId(message.getId());
                ack.setStatus(Status.FAILED);
                ack.setReceipt("Error");
            } else if (msg.contains("Client")) {
                ack.setId(message.getId());
                ack.setStatus(Status.OK);
                ack.setReceipt("/foo");
            } else {
                Thread.sleep(5000);
                ack.setId(message.getId());
                ack.setStatus(Status.OK);
                ack.setReceipt("/bar");
            }

            InputHostCommand command = new InputHostCommand();
            command.setType(InputHostCommandType.ACK);
            command.setAck(ack);
            if (session.isOpen()) {
                TListSerializer serializer = new TListSerializer(new TBinaryProtocol.Factory());
                List<TBase> list = new ArrayList<>();
                list.add(command);
                byte[] payload = serializer.serialize(list);
                try {
                    session.getRemote().sendBytes(ByteBuffer.wrap(payload));
                } catch (WebSocketException e) {
                    logger.error("Web socket send error", e);
                }
            }
        }
    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
        logger.debug("{} connected", session.getRemoteAddress().getHostName());
    }

    @OnWebSocketClose
    public void onClose(Session session, int status, String reason) {
        logger.debug("{} closed. Reason: {}", session.getRemoteAddress().getHostName(), reason);
    }
}

