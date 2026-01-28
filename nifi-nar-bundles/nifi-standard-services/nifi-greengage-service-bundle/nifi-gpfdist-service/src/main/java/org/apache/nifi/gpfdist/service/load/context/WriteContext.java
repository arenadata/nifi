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
package org.apache.nifi.gpfdist.service.load.context;

import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextId;
import org.apache.nifi.gpfdist.service.RecordProcessor;
import org.apache.nifi.gpfdist.service.RecordSink;
import org.apache.nifi.gpfdist.service.TransferDataQueryExecutor;
import org.apache.nifi.gpfdist.service.load.metadata.GpfdistLoadMetadata;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class WriteContext implements Context {
    private final ConcurrentHashMap<String, ReentrantLock> externalTableLocksBySink = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> externalTableCreatedBySink = new ConcurrentHashMap<>();
    private final ContextId contextId;
    private final Map<String, GpfdistLoadMetadata> metadataMap;
    private final Map<String, RecordSink> recordSinkMap;
    private final TransferDataQueryExecutor dropExternalTableQueryExecutor;
    private final ComponentLog logger;

    public WriteContext(final ContextId contextId,
                        List<RecordSink> recordSinks,
                        TransferDataQueryExecutor dropExternalTableQueryExecutor,
                        ComponentLog logger) {
        this.contextId = Objects.requireNonNull(contextId, "contextId");
        this.dropExternalTableQueryExecutor = dropExternalTableQueryExecutor;
        this.logger = Objects.requireNonNull(logger, "logger");
        this.metadataMap = new ConcurrentHashMap<>();
        this.recordSinkMap = new ConcurrentHashMap<>();
        Objects.requireNonNull(recordSinks, "recordSinks");

        for (RecordSink rs : recordSinks) {
            recordSinkMap.put(rs.getId(), rs);
        }
    }

    @Override
    public ContextId getContextId() {
        return contextId;
    }

    public GpfdistLoadMetadata addGpfdistLoadMetadata(String sinkId, final GpfdistLoadMetadata metadata) {
        final GpfdistLoadMetadata existing = metadataMap.putIfAbsent(sinkId, metadata);
        return existing != null ? existing : metadata;
    }

    public GpfdistLoadMetadata getGpfdistMetadata(String sinkId) {
        return metadataMap.get(sinkId);
    }

    public boolean registerRecordProcessor(RecordProcessor recordProcessor) {
        String sinkId = recordProcessor.getId().getSinkId();
        return Optional.ofNullable(recordSinkMap.get(sinkId))
                .map(rs -> rs.addRecordProcessor(recordProcessor))
                .orElse(false);
    }

    public void ensureExternalTableCreated(final String sinkId, final ThrowingRunnable runnable) throws Exception {
        Objects.requireNonNull(sinkId, "sinkId");
        Objects.requireNonNull(runnable, "runnable");

        if (isExternalTableCreated(sinkId)) {
            return;
        }

        final ReentrantLock lock = externalTableLocksBySink.computeIfAbsent(sinkId, k -> new ReentrantLock());
        lock.lock();
        try {
            if (isExternalTableCreated(sinkId)) {
                return;
            }
            runnable.run();
            externalTableCreatedBySink.put(sinkId, Boolean.TRUE);
        } finally {
            lock.unlock();
        }
    }

    public boolean isExternalTableCreated(final String sinkId) {
        return Optional.ofNullable(externalTableCreatedBySink.get(sinkId))
                .orElse(false);
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    @Override
    public void close() {
        final List<CompletableFuture<Void>> dropExternalTableFutures = new ArrayList<>();
        for (GpfdistLoadMetadata metadata : metadataMap.values()) {
            try {
                dropExternalTableFutures.add(dropExternalTableQueryExecutor.execute(metadata));
            } catch (Exception e) {
                logger.warn("Failed to submit drop external table task for metadata {}", metadata, e);
            }
        }
        try {
            CompletableFuture.allOf(dropExternalTableFutures.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            logger.warn("One or more external tables were not dropped successfully for context {}", contextId, e);
        } finally {
            metadataMap.clear();
            recordSinkMap.clear();
            externalTableCreatedBySink.clear();
            externalTableLocksBySink.clear();
            logger.info("Closed write context with id: {}", contextId);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(contextId);
    }

    @Override
    public String toString() {
        return "WriteContext{" +
                "contextId=" + contextId +
                ", sinks=" + recordSinkMap.values().stream()
                .map(Object::toString)
                .collect(Collectors.joining(",")) +
                '}';
    }
}
