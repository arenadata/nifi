package org.apache.nifi.gpfdist.service.load.metadata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LoadingResult {
    private final AtomicInteger segmentsCount = new AtomicInteger();
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final Map<Integer, LoadingSegmentResult> segmentResults = new ConcurrentHashMap<>();

    public LoadingResult() {
    }

    public AtomicInteger getSegmentsCount() {
        return segmentsCount;
    }

    public AtomicReference<Throwable> getError() {
        return error;
    }

    public Map<Integer, LoadingSegmentResult> getSegmentResults() {
        return segmentResults;
    }

    @Override
    public String toString() {
        return "LoadingResult{" +
                "segmentsCount=" + segmentsCount +
                ", segmentResults=" + segmentResults +
                getErrorIfNeeded() +
                '}';
    }

    private String getErrorIfNeeded() {
        if (error.get() != null) {
            return ", error=" + error.get().getMessage();
        }
        return "";
    }
}
