package org.apache.nifi.gpfdist.service;

public interface RecordProcessorProvider {
    boolean add(RecordProcessor processor);

    RecordProcessor take();

    void close();
}
