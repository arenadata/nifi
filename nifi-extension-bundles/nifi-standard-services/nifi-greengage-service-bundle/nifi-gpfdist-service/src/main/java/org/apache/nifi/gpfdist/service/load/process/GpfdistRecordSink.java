/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.gpfdist.service.load.process;

import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.metadata.RecordProcessorLoadingResult;
import org.apache.nifi.gpfdist.service.CancellableQuery;
import org.apache.nifi.gpfdist.service.LoadResult;
import org.apache.nifi.gpfdist.service.RecordProcessor;
import org.apache.nifi.gpfdist.service.RecordProcessorProvider;
import org.apache.nifi.gpfdist.service.RecordSink;
import org.apache.nifi.gpfdist.service.TransferDataQueryExecutor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.Record;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class GpfdistRecordSink implements RecordSink {
    private final String sinkId;
    private final ReentrantLock queueLock = new ReentrantLock();
    private final Queue<CompletableFuture<Void>> loadingRecordFutureQueue = new ArrayDeque<>();
    private final AtomicBoolean aborted = new AtomicBoolean(false);
    private final AtomicBoolean loadingStarted = new AtomicBoolean(false);
    private final AtomicBoolean inUse = new AtomicBoolean(false);
    private final ExecutorService executorService;
    private final RecordProcessorProvider recordProcessorProvider;
    private final TransferDataQueryExecutor insertDataQueryExecutor;
    private final ComponentLog logger;
    private volatile CancellableQuery insertQuery;
    private CompletableFuture<Void> insertQueryFuture;
    private String currentLoadId;


    public GpfdistRecordSink(ExecutorService executorService,
                             TransferDataQueryExecutor insertDataQueryExecutor,
                             ComponentLog logger) {
        this.sinkId = UUID.randomUUID().toString();
        this.executorService = executorService;
        recordProcessorProvider = new GpfdistRecordProcessorProvider(sinkId, logger);
        this.insertDataQueryExecutor = insertDataQueryExecutor;
        this.logger = logger;
    }

    @Override
    public String getId() {
        return sinkId;
    }

    @Override
    public void beginLoad(GpfdistMetadata loadMetadata) {
        Objects.requireNonNull(loadMetadata, "loadMetadata");
        this.currentLoadId = UUID.randomUUID().toString();
        if (aborted.get()) {
            throw new IllegalStateException("Sink is aborted: " + sinkId);
        }
        if (!loadingStarted.compareAndSet(false, true)) {
            throw new IllegalStateException("Sink is already begun loading, sinkId: " + sinkId);
        }
        insertQuery = insertDataQueryExecutor.executeCancellable(loadMetadata);
        insertQueryFuture = insertQuery.future();
        insertQueryFuture.whenComplete((v, ex) -> {
            if (ex != null) {
                logger.error("Insert query failed for sink {} - aborting load", sinkId, ex);
                abort();
            }
        });
    }

    @Override
    public void load(Record record) {
        Objects.requireNonNull(record, "record");
        if (aborted.get()) {
            throw new IllegalStateException("Cannot load record: sink aborted: " + sinkId);
        }
        if (!loadingStarted.get()) {
            throw new IllegalStateException("Cannot load record before starting beginLoad for sink: " + sinkId);
        }
        if (insertQueryFuture != null && insertQueryFuture.isCompletedExceptionally()) {
            throw new IllegalStateException("Insert already failed, refusing to load more records for sink: " + sinkId);
        }
        final CompletableFuture<Void> recordFuture = CompletableFuture.runAsync(
                () -> recordProcessorProvider.useProcessor(processor -> processor.process(record)),
                executorService);

        queueLock.lock();
        try {
            if (aborted.get()) {
                recordFuture.cancel(true);
                throw new IllegalStateException("Cannot load record: sink aborted: " + sinkId);
            }
            loadingRecordFutureQueue.add(recordFuture);
        } finally {
            queueLock.unlock();
        }
    }

    @Override
    public boolean addRecordProcessor(RecordProcessor recordProcessor) {
        return recordProcessorProvider.register(recordProcessor);
    }

    @SuppressWarnings("unchecked")
    @Override
    public CompletableFuture<LoadResult> finishLoad() {
        logger.info("Finishing loading for sink: {}", sinkId);
        final String loadIdSnapshot = currentLoadId;

        final CompletableFuture<Void>[] recordFutures;
        queueLock.lock();
        try {
            recordFutures = loadingRecordFutureQueue.toArray(new CompletableFuture[0]);
        } finally {
            queueLock.unlock();
        }
        final CompletableFuture<Void> recordsFuture = CompletableFuture.allOf(recordFutures);
        final CompletableFuture<Void> insertFuture = (insertQueryFuture != null)
                ? insertQueryFuture
                : CompletableFuture.failedFuture(new IllegalStateException("finishLoad called before beginLoad"));
        final CompletableFuture<Void> processingRecordsFuture =
                recordsFuture.thenRun(() -> {
                    try {
                        logger.info("Finished processing records for sink {}. Prepare to stop recordProcessorProvider", sinkId);
                        recordProcessorProvider.stop();
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to stop record processors", e);
                    }
                });

        return processingRecordsFuture
                .thenCompose(v -> insertFuture)
                .handle((v, ex) -> {
                    LoadResult result = getResult(recordsFuture, insertFuture, loadIdSnapshot);
                    if (ex != null) {
                        Throwable cause = getThrowableCause(ex);
                        List<Throwable> merged = new ArrayList<>(result.getErrors());
                        merged.add(cause);
                        return new LoadResult(result.getLoadId(), result.isAborted(), result.getResults(), merged);
                    }
                    return result;
                });
    }

    private LoadResult getResult(CompletableFuture<Void> recordsDone,
                                 CompletableFuture<Void> insertDone,
                                 String loadIdSnapshot) {
        final List<Throwable> errors = new ArrayList<>();
        final Throwable recordsError = unwrapFutureFailure(recordsDone);
        if (recordsError != null) {
            errors.add(recordsError);
        }
        final Throwable insertError = unwrapFutureFailure(insertDone);
        if (insertError != null) {
            errors.add(insertError);
        }
        final List<RecordProcessorLoadingResult> resultsSnapshot;
        try {
            resultsSnapshot = List.copyOf(recordProcessorProvider.getResult());
        } catch (Exception e) {
            errors.add(e);
            return new LoadResult(loadIdSnapshot, aborted.get(), List.of(), errors);
        }
        return new LoadResult(loadIdSnapshot, aborted.get(), resultsSnapshot, errors);
    }

    private static Throwable unwrapFutureFailure(final CompletableFuture<?> future) {
        try {
            future.join();
            return null;
        } catch (CancellationException ce) {
            return ce;
        } catch (Exception e) {
            return getThrowableCause(e);
        }
    }

    private static Throwable getThrowableCause(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null && (cause instanceof java.util.concurrent.CompletionException)) {
            cause = cause.getCause();
        }
        return cause;
    }

    @Override
    public void abort() {
        if (!aborted.compareAndSet(false, true)) {
            return;
        }
        logger.warn("Abort requested for sink {}", sinkId);
        cancelProcessorProvider();
        cancelRecordFutures();
        cancelInsertQuery();
    }

    private void cancelProcessorProvider() {
        try {
            recordProcessorProvider.abort();
        } catch (Exception e) {
            logger.warn("Failed to stop record processors for sink {}", sinkId, e);
        }
    }

    private void cancelRecordFutures() {
        queueLock.lock();
        try {
            for (CompletableFuture<Void> f : loadingRecordFutureQueue) {
                try {
                    f.cancel(true);
                } catch (Exception ignored) {
                }
            }
            loadingRecordFutureQueue.clear();
        } finally {
            queueLock.unlock();
        }
    }

    private void cancelInsertQuery() {
        final CancellableQuery query = insertQuery;
        if (query != null) {
            try {
                query.cancel();
            } catch (Exception e) {
                logger.warn("Failed to cancel insert query for sink {}", sinkId, e);
            }
        }
    }

    @Override
    public void resetForReuse() {
        aborted.set(false);
        loadingStarted.set(false);

        insertQueryFuture = null;
        insertQuery = null;
        currentLoadId = null;
        queueLock.lock();
        try {
            loadingRecordFutureQueue.clear();
        } finally {
            queueLock.unlock();
        }
        resetProcessorProvider();
    }

    private void resetProcessorProvider() {
        try {
            recordProcessorProvider.reset();
        } catch (Exception e) {
            logger.warn("Failed to reset recordProcessorProvider for sink {}", sinkId, e);
        }
    }

    @Override
    public void markAcquiredOrThrow() {
        if (!inUse.compareAndSet(false, true)) {
            throw new IllegalStateException("Sink already in-use: " + sinkId);
        }
    }

    @Override
    public void markReleasedOrThrow() {
        if (!inUse.compareAndSet(true, false)) {
            throw new IllegalStateException("Sink already released: " + sinkId);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(sinkId);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof GpfdistRecordSink)) return false;
        return Objects.equals(sinkId, ((GpfdistRecordSink) o).sinkId);
    }

    @Override
    public String toString() {
        return "GpfdistRecordSink{" +
                "sinkId='" + sinkId + '\'' +
                ", currentLoadId='" + currentLoadId + '\'' +
                ", loadingStarted=" + loadingStarted +
                ", aborted=" + aborted +
                '}';
    }
}
