package org.apache.nifi.gpfdist.server.request;

public class ReadableRequest {
    private final String requestId;

    public ReadableRequest(String requestId) {
        this.requestId = requestId;
    }

    public String getRequestId() {
        return requestId;
    }
}
