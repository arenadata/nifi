package org.apache.nifi.gpfdist.service;

import org.apache.nifi.serialization.record.Record;

public interface RecordProcessor {
    void process(Record record);

    void stop();
}
