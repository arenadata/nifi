package org.apache.nifi.gpfdist.server.config;

public class GpfdistServerConfig {
    private final int port;
    private final String host;
    private final int minThreads;
    private final int maxThreads;
    private final int idleTimeoutMs;
    private final boolean isSslEnabled;

    public GpfdistServerConfig(int port,
                               String host,
                               int minThreads,
                               int maxThreads,
                               int idleTimeoutMs,
                               boolean isSslEnabled) {
        this.port = port;
        this.host = host;
        this.minThreads = minThreads;
        this.maxThreads = maxThreads;
        this.idleTimeoutMs = idleTimeoutMs;
        this.isSslEnabled = isSslEnabled;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    public boolean isSslEnabled() {
        return isSslEnabled;
    }

    public int getMinThreads() {
        return minThreads;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public int getIdleTimeoutMs() {
        return idleTimeoutMs;
    }
}
