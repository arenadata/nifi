package org.apache.nifi.gpfdist.service.load.context;

import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextId;
import org.apache.nifi.gpfdist.service.RecordProcessorProvider;
import org.apache.nifi.gpfdist.service.load.metadata.GpfdistLoadMetadata;
import org.apache.nifi.logging.ComponentLog;

import java.util.concurrent.atomic.AtomicReference;

public class WriteContext implements Context {
    private final ContextId contextId;
    private final int bufferSize;
    private final GpfdistLoadMetadata metadata;
    private final RecordProcessorProvider recordProcessorProvider;
    private final ComponentLog logger;
    private final AtomicReference<Throwable> error = new AtomicReference<>();

    public WriteContext(final ContextId contextId,
                        int bufferSize,
                        final GpfdistLoadMetadata metadata,
                        final RecordProcessorProvider recordProcessorProvider,
                        ComponentLog logger) {
        this.contextId = contextId;
        this.bufferSize = bufferSize;
        this.metadata = metadata;
        this.recordProcessorProvider = recordProcessorProvider;
        this.logger = logger;
    }

    @Override
    public ContextId getContextId() {
        return contextId;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public GpfdistLoadMetadata getMetadata() {
        return metadata;
    }

    public AtomicReference<Throwable> getError() {
        return error;
    }

    public RecordProcessorProvider getRecordProcessorProvider() {
        return recordProcessorProvider;
    }

    public ComponentLog getLogger() {
        return logger;
    }

    @Override
    public void close() {
        recordProcessorProvider.close();
        logger.debug("Closed write context {}", contextId);
    }

    @Override
    public String toString() {
        return "WriteContext{" +
                "contextId=" + contextId.getId() +
                '}';
    }
}
