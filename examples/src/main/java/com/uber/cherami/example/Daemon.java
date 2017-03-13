package com.uber.cherami.example;

/**
 * Defines the contract for a daemon thread.
 *
 * @author venkat
 */
public interface Daemon {
    /**
     * Starts the daemon.
     */
    void start();

    /**
     * Stops the daemon.
     */
    void stop();
}
