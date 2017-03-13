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

/**
 * Immutable value type containing the configuration for the demo.
 *
 * @author venkat
 */
public class Config {

    /** Cherami server endpoint ip. */
    public final String ip;
    /** Cherami server endpoint port. */
    public final int port;

    /** Cherami destination path. */
    public final String destinationPath;
    /** Cherami consumer group name. */
    public final String consumergroupName;

    /** Number of publishers. */
    public final int nPublishers;
    /** Number of consumers. */
    public final int nConsumers;

    /** Number of messages to send. */
    public final int nMessagesToSend;
    /** Size of the published messages. */
    public final int messageSize;

    /** True for async publish/consume. */
    public final boolean useAsync;

    Config(String ip, int port, String dstPath, String cgName, int nPublishers, int nConsumers, int nMessagesToSend,
            int messageSize, boolean useAsync) {
        this.ip = ip;
        this.port = port;
        this.destinationPath = dstPath;
        this.consumergroupName = cgName;
        this.nPublishers = nPublishers;
        this.nConsumers = nConsumers;
        this.nMessagesToSend = nMessagesToSend / nPublishers;
        this.messageSize = messageSize;
        this.useAsync = useAsync;
    }

    private static void printHelp() {
        System.out.println("Usage: java com.uber.cherami.example.Demo --nMsgsToSend=[nMsgsToSend] --msgSize=[msgSize] "
                        + "[--endpoint=[frontEndIP:Port] ] [ --nPublishers=[nPublishers] ] "
                        + "[ --nConsumers=[nConsumers] ] [ --useAsync=true ]");
    }

    private static void parseError(String arg) {
        System.out.println("ParseError: " + arg);
        printHelp();
        exit(1);
    }

    /**
     * Parses the given commandline args and converts them into a Config object.
     *
     * @param args
     *            String array, representing the command line args
     * @return Config object, on success.
     */
    public static Config parse(String[] args) {

        if (args.length < 2) {
            parseError("Not enough arguments");
        }

        int send = 0;
        int size = 0;
        int port = 0;
        int nPublishers = 1;
        int nConsumers = 1;
        boolean useAsync = false;

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
                case "--msgSize":
                    size = Integer.parseInt(nameValue[1]);
                    break;
                case "--nPublishers":
                    nPublishers = Integer.parseInt(nameValue[1]);
                    break;
                case "--nConsumers":
                    nConsumers = Integer.parseInt(nameValue[1]);
                    break;
                case "--useAsync":
                    useAsync = true;
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
            parseError("--nMsgsToSend and --msgSize must be greater than zero");
        }

        String dstPath = String.format("/test/java.example_%d", new Random().nextInt(Integer.MAX_VALUE));
        String cgName = String.format("%s_reader", dstPath);

        return new Config(ip, port, dstPath, cgName, nPublishers, nConsumers, send, size, useAsync);
    }
}
