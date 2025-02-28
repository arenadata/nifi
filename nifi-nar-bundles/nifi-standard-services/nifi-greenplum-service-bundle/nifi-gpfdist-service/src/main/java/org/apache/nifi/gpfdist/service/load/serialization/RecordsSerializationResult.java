package org.apache.nifi.gpfdist.service.load.serialization;

public class RecordsSerializationResult {
    private final byte[] data;
    private final long rowCount;

    public RecordsSerializationResult(byte[] data, long rowCount) {
        this.data = data;
        this.rowCount = rowCount;
    }

    public byte[] getData() {
        return data;
    }

    public long getRowCount() {
        return rowCount;
    }
}
