package org.apache.nifi.gpfdist.service;

import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.serialization.record.Record;

import java.util.concurrent.CompletableFuture;

public interface RecordSink {
    void load(Record record);

    CompletableFuture<Void> finish();

    void abort();

    Context getContext();
}
