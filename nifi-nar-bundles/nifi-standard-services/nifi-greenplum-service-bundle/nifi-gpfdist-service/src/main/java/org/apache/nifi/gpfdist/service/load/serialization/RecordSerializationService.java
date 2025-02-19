package org.apache.nifi.gpfdist.service.load.serialization;

import org.apache.nifi.serialization.record.Record;

public interface RecordSerializationService {
    void append(Record record);

    int getSerializedRecordsCount();

    byte[] toByteArray();

    void reset();

    void close();
}
