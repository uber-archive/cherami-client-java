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
package com.uber.cherami.example;

import static java.lang.System.exit;

import java.util.Random;

public class Config {
    public final int numMessagesToSend;
    public final int maxNumberToReceive;
    public final int messageSize;
    public final String destinationPath;
    public final String consumerName;
    public final String ip;
    public final int port;

    Config(int numMessages, int numToReceive, int messageSize, String consumerName, String path,
            String ip, int port) {
        this.numMessagesToSend = numMessages;
        this.maxNumberToReceive = numToReceive;
        this.messageSize = messageSize;
        this.consumerName = consumerName;
        this.destinationPath = path;
        this.port = port;
        this.ip = ip;
    }

    private static void printHelp() {
        System.out.println(
                "Usage: java com.uber.cherami.example.Example --nMsgsToSend=[nMsgsToSend] --msgSize=[msgSize] [ --nMsgsToReceive=[nMsgsToReceive] ]  [--endpoint=[frontEndIP:Port] ]");
    }

    private static void parseError(String arg) {
        System.out.println("ParseError: " + arg);
        printHelp();
        exit(1);
    }

    public static Config parse(String[] args) {

        if (args.length < 2) {
            parseError("Not enough arguments");
        }

        int send = 0;
        int receive = 0;
        int size = 0;
        int port = 0;
        String ip = "";

        try {
            for (int i = 0; i < args.length; i++) {

                String[] nameValue = args[i].split("=");
                if (nameValue.length != 2) {
                    parseError(nameValue[0]);
                }

                switch (nameValue[0]) {
                case "--nMsgsToSend":
                    send = Integer.parseInt(nameValue[1]);
                    break;
                case "--nMsgsToReceive":
                    receive = Integer.parseInt(nameValue[1]);
                    break;
                case "--msgSize":
                    size = Integer.parseInt(nameValue[1]);
                    break;
                case "--endpoint":
                    String[] ipPort = nameValue[1].split(":");
                    if (ipPort.length != 2) {
                        parseError(nameValue[0]);
                    }
                    ip = ipPort[0];
                    port = Integer.parseInt(ipPort[1]);
                    break;
                default:
                    parseError("Unknown arg " + nameValue[0]);
                }
            }
        } catch (Exception e) {
            parseError(e.getMessage());
        }

        if (send == 0 || size == 0) {
            parseError("--send and --size must be greater than zero");
        }

        String destinationPath = String.format("/test/java.example_%d", new Random().nextInt(Integer.MAX_VALUE));
        String consumerName = String.format("%s_reader", destinationPath);

        return new Config(send, receive, size, consumerName, destinationPath, ip, port);
    }
}
