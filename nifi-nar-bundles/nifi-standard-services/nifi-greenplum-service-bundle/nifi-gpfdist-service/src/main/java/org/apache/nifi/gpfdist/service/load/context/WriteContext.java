package org.apache.nifi.gpfdist.service.load.context;

import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextId;
import org.apache.nifi.gpfdist.service.RecordProcessorProvider;
import org.apache.nifi.gpfdist.service.load.metadata.GpfdistLoadMetadata;
import org.apache.nifi.gpfdist.service.load.metadata.LoadingResult;
import org.apache.nifi.logging.ComponentLog;

public class WriteContext implements Context {
    private final ContextId contextId;
    private final int bufferSize;
    private final GpfdistLoadMetadata metadata;
    private final RecordProcessorProvider recordProcessorProvider;
    private final ComponentLog logger;
    private final LoadingResult result;

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
        this.result = new LoadingResult();
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

    public LoadingResult getResult() {
        return result;
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
                "contextId=" + contextId +
                ", result=" + result +
                '}';
    }
}
