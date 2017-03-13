package com.uber.cherami.example;

import com.uber.cherami.client.CheramiClient;

/**
 * Value type that holds the context for demo.
 *
 * @author venkat
 */
public class Context {
    /** Config representing the configuration for the demo. */
    public final Config config;
    /** Stats containing the metrics emitted by the demo. */
    public final Stats stats;
    /** Cherami client for publish/consume. */
    public final CheramiClient client;

    /**
     * Constructs and returns a Context object.
     */
    public Context(Config config, CheramiClient client) {
        this.config = config;
        this.stats = new Stats();
        this.client = client;
    }
}
