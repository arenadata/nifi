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

import org.apache.nifi.gpfdist.service.RecordSink;
import org.apache.nifi.gpfdist.service.TransferDataQueryExecutor;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class GpfdistRecordSinkManager {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final int localPoolSize;
    private final BlockingQueue<RecordSink> recordPool;
    private final List<RecordSink> recordSinks;
    private final ExecutorService executorService;
    private final TransferDataQueryExecutor insertDataQueryExecutor;
    private final ComponentLog logger;

    public GpfdistRecordSinkManager(int segmentCount,
                                    int segmentConcurrencyMultiplier,
                                    int concurrentTasksPerNode,
                                    int nodeCount,
                                    int nodeIndex,
                                    TransferDataQueryExecutor insertDataQueryExecutor,
                                    ExecutorService executorService,
                                    ComponentLog logger) {
        validateInputs(segmentCount, segmentConcurrencyMultiplier, concurrentTasksPerNode, nodeCount, nodeIndex);
        final int limitByGreengage = Math.max(1, segmentCount * segmentConcurrencyMultiplier);
        final int limitByNifi = Math.max(1, concurrentTasksPerNode * nodeCount);
        final int clusterPoolSize = Math.min(limitByGreengage, limitByNifi);
        final int base = clusterPoolSize / nodeCount;
        final int remainder = clusterPoolSize % nodeCount;
        final int calculatedLocalPoolSize = base + (nodeIndex < remainder ? 1 : 0);
        this.localPoolSize = Math.max(0, calculatedLocalPoolSize);
        this.recordPool = new LinkedBlockingQueue<>(localPoolSize == 0 ? 1 : localPoolSize);
        this.recordSinks = new ArrayList<>(localPoolSize);
        this.insertDataQueryExecutor = insertDataQueryExecutor;
        this.executorService = executorService;
        this.logger = logger;
        initRecordSinks();
    }

    private void initRecordSinks() {
        try {
            for (int i = 0; i < localPoolSize; i++) {
                RecordSink sink = new GpfdistRecordSink(executorService, insertDataQueryExecutor, logger);
                recordSinks.add(sink);
                recordPool.offer(sink);
            }
        } catch (Exception e) {
            abortAll(recordSinks);
            throw new RuntimeException("Failed to initialize RecordSink pool", e);
        }
    }

    public RecordSink acquire() throws InterruptedException {
        ensureOpen();
        RecordSink sink = recordPool.take();
        sink.markAcquiredOrThrow();
        return sink;
    }

    public void release(RecordSink sink) {
        Objects.requireNonNull(sink, "sink");
        ensureOpen();
        sink.markReleasedOrThrow();
        if (closed.get()) {
            return;
        }
        resetOrThrow(sink);
        if (!recordPool.offer(sink)) {
            throw new IllegalStateException("Failed to release RecordSink back to pool: " + sink.getId());
        }
    }

    private static void validateInputs(int segmentCount,
                                       int segmentConcurrencyMultiplier,
                                       int concurrentTasksPerNode,
                                       int nodeCount,
                                       int nodeIndex) {
        if (segmentCount <= 0) {
            throw new IllegalArgumentException("segmentCount must be > 0");
        }
        if (segmentConcurrencyMultiplier <= 0) {
            throw new IllegalArgumentException("segmentConcurrencyMultiplier must be > 0");
        }
        if (concurrentTasksPerNode <= 0) {
            throw new IllegalArgumentException("concurrentTasksPerNode must be > 0");
        }
        if (nodeCount <= 0) {
            throw new IllegalArgumentException("nodeCount must be > 0");
        }
        if (nodeIndex < 0 || nodeIndex >= nodeCount) {
            throw new IllegalArgumentException(
                    "nodeIndex must be in range [0.." + (nodeCount - 1) + "]"
            );
        }
    }

    public List<RecordSink> getRecordSinks() {
        return Collections.unmodifiableList(recordSinks);
    }

    public int size() {
        return recordSinks.size();
    }

    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        recordPool.clear();
        abortAll(recordSinks);
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("GpfdistRecordSinkManager is closed");
        }
    }

    private void resetOrThrow(RecordSink sink) {
        try {
            sink.resetForReuse();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to reset sink for reuse: " + sink.getId(), e);
        }
    }

    private void safeAbort(RecordSink sink) {
        try {
            sink.abort();
        } catch (Exception e) {
            logger.warn("Failed to abort sink {}", sink.getId(), e);
        }
    }

    private void abortAll(List<RecordSink> sinks) {
        for (RecordSink sink : sinks) {
            safeAbort(sink);
        }
    }
}
