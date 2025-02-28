package org.apache.nifi.gpfdist.server;

public interface GpfdistServer {
    void start();

    void stop();

    int getPort();

    String getHost();
}
