package org.apache.nifi.gpfdist.service.load.process;

import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextId;
import org.apache.nifi.gpfdist.service.RecordProcessor;
import org.apache.nifi.gpfdist.service.RecordSink;
import org.apache.nifi.gpfdist.service.load.context.WriteContext;
import org.apache.nifi.gpfdist.service.load.context.WriteContextManager;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.Record;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class GpfdistRecordSink implements RecordSink {
    private static final int PROCESSING_RECORD_TIMEOUT_MS = 60000;
    private final WriteContext writeContext;
    private final ExecutorService executorService;
    private final WriteContextManager contextManager;
    private final Queue<CompletableFuture<Void>> loadingRecordfutureQueue = new LinkedList<>();
    private final ComponentLog logger;

    public GpfdistRecordSink(final ContextId contextId,
                             final ExecutorService executorService,
                             final WriteContextManager contextManager,
                             ComponentLog logger) {
        this.contextManager = contextManager;
        this.writeContext = contextManager.get(contextId)
                .orElseThrow(() -> new IllegalArgumentException("No write context found for " + contextId));
        this.executorService = executorService;
        this.logger = logger;
    }

    @Override
    public void load(Record record) {
        CompletableFuture<Void> recordFuture = CompletableFuture.runAsync(() -> {
            RecordProcessor recordProcessor = writeContext.getRecordProcessorProvider().take();
            recordProcessor.process(record);
            writeContext.getRecordProcessorProvider().add(recordProcessor);
        }, executorService);
        loadingRecordfutureQueue.add(recordFuture);
    }

    @Override
    public CompletableFuture<Void> finish() {
        return CompletableFuture.runAsync(() -> {
            CompletableFuture<Void> future;
            while ((future = loadingRecordfutureQueue.poll()) != null) {
                try {
                    future.get(PROCESSING_RECORD_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to process record", e);
                }
            }
            logger.debug("Finished loading records within context {}", writeContext.getContextId());
            writeContext.close();
            contextManager.remove(writeContext.getContextId());
        }, executorService);
    }

    @Override
    public void abort() {
        String errMsg = format("Loading data within context %s is aborted", writeContext.getContextId());
        failContext(new RuntimeException(errMsg));
        logger.warn(errMsg);
    }

    private void failContext(Throwable e) {
        try {
            loadingRecordfutureQueue.clear();
            writeContext.getError().set(e);
            writeContext.close();
        } finally {
            contextManager.remove(writeContext.getContextId());
        }
    }

    @Override
    public Context getContext() {
        return writeContext;
    }
}
