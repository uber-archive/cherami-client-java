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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.cherami.ChecksumOption;
import com.uber.cherami.HostAddress;
import com.uber.cherami.client.ConnectionManager.Connection;

/**
 * Manages the lifecycle of streaming connections.
 *
 * This class implements the common connection handling logic needed by cherami
 * publishers and consumers. For example, when a publisher is created for a
 * destination, the publisher first needs to discover the publish endpoints
 * advertised by the server and initiate tcp connections to all those endpoints.
 * Subsequently, it needs to periodically refresh its set of cached endpoints
 * with the server, create conns for the newly discovered endpoints and remove
 * conns that are no longer advertised by the server. The latter part is called
 * the reconfiguration protocol. Likewise, the consumer needs to do the same
 * connection handling logic.
 *
 * This class takes two things as input viz. EndpointFinder, for discovering the
 * endpoints and a ConnectionFactory, for creating a connection from an
 * endpoint. It then manages the lifecycle of all the conns advertised by the
 * server.
 *
 * @author venkat
 *
 * @param <T>
 *            Type of connection managed by this manager.
 */
public class ConnectionManager<T extends Connection> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);

    /**
     * Max number of threads that the thread pool will hold onto even if they
     * are idle i.e. this is the number of idle threads that the thread pool
     * will maintain in the cache to immediately execute tasks as they are
     * submitted.
     *
     * The value is thisThread(1) + nAvgExpectedConns (4).
     */
    private static final int THREAD_POOL_CORE_SIZE = 5;

    private static final long RECONFIG_INTERVAL_MILLIS = 10 * 1000;

    private final String name;
    private final CountDownLatch quitter;
    private final CountDownLatch stopped;
    private final ConnectionFactory<T> factory;
    private final EndpointFinder endpointFinder;
    private final AtomicReference<State<T>> stateRef;
    private final AtomicBoolean isOpened;
    private final AtomicBoolean isClosed;
    private final AtomicBoolean isRefreshRequested;
    private final ExecutorService executor;

    private long nextReconfigTimeMillis;

    /**
     * Creates and returns a ConnectionManager object.
     *
     * @param name
     *            Name for the connection manager, used for logging.
     * @param factory
     *            ConnectionFactory, for creating new conns of type T
     * @param endpointFinder
     *            EndpointFinder for polling the server to discover endpoints.
     */
    public ConnectionManager(String name, ConnectionFactory<T> factory, EndpointFinder endpointFinder) {
        this.factory = factory;
        this.endpointFinder = endpointFinder;
        this.quitter = new CountDownLatch(1);
        this.stopped = new CountDownLatch(1);
        this.stateRef = new AtomicReference<State<T>>(new State<T>());
        this.isRefreshRequested = new AtomicBoolean(false);
        this.isOpened = new AtomicBoolean(false);
        this.isClosed = new AtomicBoolean(false);
        this.name = String.format("[%s]", name);
        this.executor = new ThreadPoolExecutor(THREAD_POOL_CORE_SIZE, Integer.MAX_VALUE, 0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>());
    }

    /**
     * Opens the connection manager.
     *
     * Initiates initial discovery and conn creation.
     *
     * @throws InterruptedException
     *             In receiving an interrupt.
     */
    public synchronized void open() throws InterruptedException {

        if (isOpened.get()) {
            return;
        }

        final int maxRetries = 3;
        final long retryIntervalMillis = 1000;

        EndpointsInfo info = new EndpointsInfo();
        for (int i = 0; i <= maxRetries; i++) {
            try {
                info = endpointFinder.find();
            } catch (IOException e) {
                logger.warn("{}: Initial endpoint discovery failed", name, e);
                if (quitter.await(retryIntervalMillis, TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
        }

        if (!info.hostAddrs.isEmpty()) {
            repairConnections(info);
            nextReconfigTimeMillis = now() + RECONFIG_INTERVAL_MILLIS;
        } else {
            // don't retry immediately
            nextReconfigTimeMillis = now() + (RECONFIG_INTERVAL_MILLIS / 2);
        }

        Thread thread = new Thread(this);
        thread.setName(name + "-connection-manager");
        thread.setDaemon(true);
        executor.execute(thread);

        isOpened.set(true);
        logger.info("{}: ConnectionManager open", name);
    }

    /**
     * Closes the connection manager.
     */
    public synchronized void close() {
        if (isClosed.get()) {
            return;
        }
        isClosed.set(true);
        quitter.countDown();
        awaitStop();
        State<T> state = stateRef.getAndSet(new State<T>());
        for (Connection conn : state.connections) {
            conn.close();
        }
        executor.shutdown();
        logger.info("{}: ConnectionManager closed", name);
    }

    /**
     * @return Returns a list of active connections.
     *
     * @throws ConnectionManagerClosedException
     *             If the connection manager is already closed.
     */
    public List<T> getConnections() throws ConnectionManagerClosedException {
        if (isClosed.get()) {
            throw new ConnectionManagerClosedException();
        }
        return this.stateRef.get().connections;
    }

    /**
     * Returns a map of address to connection object.
     *
     * @return Map<String,T> representing map of address to connection
     * @throws ConnectionManagerClosedException
     */
    public Map<String, T> getAddrToConnectionMap() throws ConnectionManagerClosedException {
        if (isClosed.get()) {
            throw new ConnectionManagerClosedException();
        }
        return this.stateRef.get().addrToConnection;
    }

    /**
     * Forces an endpoint refresh.
     */
    public void refreshNow() {
        this.isRefreshRequested.set(true);
    }

    /**
     * Interface for endpoint discovery.
     *
     * @author venkat
     */
    public interface EndpointFinder {
        /**
         * @return List of host addresses to connect to.
         * @throws IOException
         *             On I/O errors. These errors are treated as retryable.
         */
        EndpointsInfo find() throws IOException;
    }

    /**
     * Generic interface to be supported by connections that are managed by this
     * connection manager.
     *
     * @author venkat
     */
    public interface Connection {
        /**
         * @return True if the conn is open, false otherwise.
         */
        boolean isOpen();
        /**
         * Closes the connection.
         */
        void close();
    }

    /**
     * Value type which serves as the result of endpoint discovery.
     *
     * @author venkat
     */
    public static class EndpointsInfo {
        /** Port to be used for rpc calls to any of hostAddrs. */
        public final int rpcPort;
        /** Type of checksum, examples are crc32, md5. */
        public final ChecksumOption checksumOption;
        /** List of ip:port endpoints to be used for data streaming. */
        public final List<HostAddress> hostAddrs;

        /**
         * Creates an EndpointsInfo object with defaults.
         */
        public EndpointsInfo() {
            this.rpcPort = 0;
            this.checksumOption = ChecksumOption.CRC32IEEE;
            this.hostAddrs = new ArrayList<>();
        }

        /**
         * Creates and EndpointsInfo object.
         *
         * @param rpcPort
         *            int representing the port to be used for rpc calls to any
         *            of the specified hostAddrs endpoints.
         * @param hostAddrs
         *            List of host addresses to be used for data streaming.
         * @param checksumOption
         *            The checksum option to be used for the connections. This
         *            value will be passed to the connection factory.
         */
        public EndpointsInfo(int rpcPort, List<HostAddress> hostAddrs, ChecksumOption checksumOption) {
            this.rpcPort = rpcPort;
            this.hostAddrs = hostAddrs;
            this.checksumOption = checksumOption;
        }
    }

    /**
     * A factory that can create connections of any type.
     *
     * @author venkat
     *
     * @param <T>
     */
    public interface ConnectionFactory<T extends Connection> {
        /**
         * Creates and returns a new connection.
         *
         * @param host
         *            Host name or ip address of the endpoint.
         * @param dataPort
         *            Port number to be used for streaming data.
         * @param rpcPort
         *            Port number to be used for rpc.
         * @param option
         *            Checksum option to be used by this connection.
         * @return Connection object.
         *
         * @throws InterruptedException
         *             On an interrupt.
         * @throws IOException
         *             If connection creation fails.
         */
        T create(String host, int dataPort, int rpcPort, ChecksumOption option)
                throws InterruptedException, IOException;
    }

    /**
     * Exception that is thrown after the conn manager is closed.
     *
     * @author venkat
     *
     */
    @SuppressWarnings("serial")
    public static class ConnectionManagerClosedException extends Exception {
    }

    @Override
    public void run() {
        try {

            while (!isClosing()) {

                if (isRefreshRequested.get() || isPeriodicReconfigDue()) {

                    isRefreshRequested.set(false);

                    EndpointsInfo info = new EndpointsInfo();
                    try {
                        info = endpointFinder.find();
                        nextReconfigTimeMillis = now() + RECONFIG_INTERVAL_MILLIS;
                        logger.debug("{}: Refreshed endpoints, new endpoints={}", name, info.hostAddrs);
                    } catch (IOException e) {
                        // only I/O exception is retryable, just log and move on
                        nextReconfigTimeMillis = now() + (RECONFIG_INTERVAL_MILLIS / 2);
                        logger.warn("{}: Error discovering endpoints.", name, e);
                    }

                    if (info.hostAddrs.isEmpty()) {
                        State<T> state = stateRef.get();
                        repairConnections(state.addrs, state.rpcPort, state.checksumOption);
                    } else {
                        repairConnections(info);
                    }
                }

                // the sleep millis here directly determines the max
                // time after which we discover an extent after its
                // made available by the service side.
                sleepMillis(1000);
            }
        } catch (Throwable e) {
            stopped.countDown();
            close();
            logger.error("{}: Caught unexpected exception, thread exiting", name, e);
        } finally {
            stopped.countDown();
        }
    }

    private void sleepMillis(long millis) {
        try {
            quitter.await(millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }
    }

    private void awaitStop() {
        try {
            stopped.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            //
        }
    }

    private long now() {
        return System.currentTimeMillis();
    }

    private boolean isPeriodicReconfigDue() {
        return (now() >= nextReconfigTimeMillis);
    }

    private void repairConnections(EndpointsInfo info) {
        Set<String> endpoints = new HashSet<String>();
        for (HostAddress host : info.hostAddrs) {
            String key = String.format("%s:%d", host.getHost(), host.getPort());
            endpoints.add(key);
        }
        repairConnections(endpoints, info.rpcPort, info.checksumOption);
    }

    private boolean isClosing() {
        return quitter.getCount() < 1;
    }

    /**
     * Initiates connection to given list of ip:ports in parallel.
     *
     * As each connection succeeds, its added to the existing set of endpoints
     * and the state is updated atomically.
     */
    private void connectAll(List<String> connectAddrs, int rpcPort, ChecksumOption checksumOption) {

        if (connectAddrs.size() == 0) {
            return;
        }

        List<Future<T>> futures = new ArrayList<>(connectAddrs.size());
        for (String ep : connectAddrs) {
            // kick off async connect to all endpoints
            Future<T> future = executor.submit(new Connector<T>(ep, rpcPort, checksumOption, factory));
            futures.add(future);
        }

        State<T> state = stateRef.get();
        Set<String> allAddrs = state.addrs;
        Map<String, T> addrToConns = state.addrToConnection;

        int remaining = futures.size();

        while (remaining > 0) {
            // wait for all async connects to either succeed
            // or throw an executionexception (connectTimeout)
            for (int i = 0; i < futures.size(); i++) {
                Future<T> future = futures.get(i);
                if (future == null) {
                    continue;
                }
                try {
                    T conn = future.get(500, TimeUnit.MILLISECONDS);
                    if (conn != null) {
                        addrToConns = new HashMap<>(addrToConns);
                        addrToConns.put(connectAddrs.get(i), conn);
                        State<T> newState = new State<T>(allAddrs, addrToConns, rpcPort, checksumOption);
                        this.stateRef.set(newState);
                    }
                    // null out the completed futures
                    futures.set(i, null);
                    remaining--;
                } catch (TimeoutException | InterruptedException e) {
                    continue;
                } catch (Exception e) {
                    futures.set(i, null);
                    remaining--;
                    logger.error("{}: Error connecting to endpoint {}", name, connectAddrs.get(i), e);
                }
            }
        }
    }

    /**
     * Given a list of desired publish endpoints, creates/adds endpoints that
     * previously didn't exist and removes endpoints that are no longer needed.
     *
     * The algorithm is as follows:
     * <p>
     * (1) Compute the endpoints that are removed <br>
     * (2) Compute the endpoints that are new <br>
     * (3) Create a copy of the existing state and delete the removed endpoints
     * <br>
     * (4) Atomically update the state (with the removed endpoints) <br>
     * (5) Initiate tcp conn to all new endpoints in parallel <br>
     * (6) As each endpoint is connected, copy/add endpoint to state and update
     * atomically.
     */
    private void repairConnections(Set<String> desiredEndpoints, int rpcPort, ChecksumOption checksumOption) {

        if (isClosing()) {
            return;
        }

        if (desiredEndpoints.isEmpty()) {
            logger.info("{}: Got empty desired endpoints", this.name);
        }

        State<T> currState = stateRef.get();
        // lazily initialize this as needed
        Map<String, T> newAddrToConnections = null;

        // compute the endpoints that are removed
        if (currState.addrs != desiredEndpoints) {

            if (newAddrToConnections == null) {
                newAddrToConnections = new HashMap<>(currState.addrToConnection);
            }

            // only do a set difference if the desired endpoints
            // is different from the existing set of endpoints
            for (String oldAddr : currState.addrs) {
                if (desiredEndpoints.contains(oldAddr)) {
                    continue;
                }
                // no longer needed, close the old conn
                Connection conn = currState.addrToConnection.get(oldAddr);
                if (conn != null) {
                    conn.close();
                    newAddrToConnections.remove(oldAddr);
                }
                logger.info("{}: Removed endpoint, endpoint={}", name, oldAddr);
            }
        }

        List<String> connectAddrs = new ArrayList<>(desiredEndpoints.size());

        // compute the endpoints that are new or pending conn initiation
        for (String addr : desiredEndpoints) {
            T conn = currState.addrToConnection.get(addr);
            if (conn != null) {
                if (!conn.isOpen()) {
                    connectAddrs.add(addr);
                    if (newAddrToConnections == null) {
                        newAddrToConnections = new HashMap<>(currState.addrToConnection);
                    }
                    newAddrToConnections.remove(addr);
                }
                continue;
            } 
            if (!currState.addrs.contains(addr)) {
                // only log if an addr is discovered for the first time
                logger.info("{}: Added endpoint, endpoint={}", name, addr);
            }
            connectAddrs.add(addr);
        }

        if (newAddrToConnections != null) {
            // Something got changed, update the state before proceeding
            State<T> newState = new State<T>(desiredEndpoints, newAddrToConnections, rpcPort, checksumOption);
            this.stateRef.set(newState);
            currState = newState;
        }

        // now connect to all new endpoints
        connectAll(connectAddrs, rpcPort, checksumOption);
    }

    /**
     * Initiates a connection asynchronously.
     *
     * @author venkat
     *
     * @param <T>
     */
    private static class Connector<T extends Connection> implements Callable<T> {

        private final String addr;
        private final int rpcPort;
        private final ChecksumOption checksumOption;
        private final ConnectionFactory<T> factory;

        Connector(String addr, int rpcPort, ChecksumOption checksumOption, ConnectionFactory<T> factory) {
            this.addr = addr;
            this.rpcPort = rpcPort;
            this.checksumOption = checksumOption;
            this.factory = factory;
        }

        private HostAddress toHostAddr(String ep) {
            try {
                String[] hostPort = ep.split(":");
                String host = hostPort[0];
                int port = Integer.parseInt(hostPort[1]);
                return new HostAddress().setHost(host).setPort(port);
            } catch (Exception e) {
                throw new RuntimeException("Malformed endpoint " + ep, e);
            }
        }

        @Override
        public T call() throws Exception {
            HostAddress hostAddr = toHostAddr(addr);
            if (hostAddr == null) {
                return null;
            }
            return factory.create(hostAddr.getHost(), hostAddr.getPort(), rpcPort, checksumOption);
        }
    }

    /**
     * Value type that holds the set of connection manager state.
     *
     * @author venkat
     */
    private static class State<T extends Connection> {

        public final int rpcPort;
        public final Set<String> addrs;
        public final List<T> connections;
        public final ChecksumOption checksumOption;
        public final Map<String, T> addrToConnection;

        State() {
            this.rpcPort = 0;
            this.addrs = new HashSet<String>(4);
            this.addrToConnection = new HashMap<String, T>(4);
            this.connections = new ArrayList<T>();
            this.checksumOption = ChecksumOption.CRC32IEEE;
        }

        State(Set<String> addrs, Map<String, T> addrToConnection, int rpcPort, ChecksumOption checksumOption) {
            this.addrs = addrs;
            this.rpcPort = rpcPort;
            this.checksumOption = checksumOption;
            this.addrToConnection = addrToConnection;
            this.connections = new ArrayList<>(addrToConnection.values());
        }
    }
}
