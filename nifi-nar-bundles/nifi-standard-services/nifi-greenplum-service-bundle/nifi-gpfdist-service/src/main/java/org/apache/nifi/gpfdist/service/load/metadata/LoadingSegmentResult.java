package org.apache.nifi.gpfdist.service.load.metadata;

import java.util.Objects;

public class LoadingSegmentResult {
    private final int segmentId;
    private final long loadedRecords;
    private final long loadedBytes;

    public LoadingSegmentResult(int segmentId, long loadedRecords, long loadedBytes) {
        this.segmentId = segmentId;
        this.loadedRecords = loadedRecords;
        this.loadedBytes = loadedBytes;
    }

    public int getSegmentId() {
        return segmentId;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        LoadingSegmentResult that = (LoadingSegmentResult) o;
        return segmentId == that.segmentId && loadedRecords == that.loadedRecords && loadedBytes == that.loadedBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentId, loadedRecords, loadedBytes);
    }

    @Override
    public String toString() {
        return "LoadingSegmentResult{" +
                "segmentId=" + segmentId +
                ", loadedRecords=" + loadedRecords +
                ", loadedBytes=" + loadedBytes +
                '}';
    }
}
