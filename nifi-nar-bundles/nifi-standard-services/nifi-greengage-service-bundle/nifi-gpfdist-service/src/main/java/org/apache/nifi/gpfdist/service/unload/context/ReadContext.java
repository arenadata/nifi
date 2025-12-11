/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.gpfdist.service.unload.context;

import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextId;
import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.service.unload.process.RecordProcessingService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class ReadContext implements Context {
    private final ContextId contextId;
    private final int globalParallelFactor;
    private final RecordSchema recordSchema;
    private final Map<String, ColumnDataType> dataTypes;
    private final Map<String, GpfdistMetadata> metadataMap;
    private final Map<String, RecordProcessingService> recordProcessingServiceMap;
    private final Map<String, CompletableFuture<Void>> unloadQueryFutureMap;
    private final ComponentLog logger;

    public ReadContext(ContextId contextId,
                       int globalParallelFactor,
                       RecordSchema recordSchema,
                       Map<String, ColumnDataType> dataTypes,
                       Map<String, GpfdistMetadata> metadataMap,
                       Map<String, RecordProcessingService> recordProcessingServiceMap,
                       ComponentLog logger) {
        this.contextId = contextId;
        this.globalParallelFactor = globalParallelFactor;
        this.recordSchema = recordSchema;
        this.metadataMap = metadataMap;
        this.recordProcessingServiceMap = recordProcessingServiceMap;
        this.dataTypes = dataTypes;
        unloadQueryFutureMap = new ConcurrentHashMap<>();
        this.logger = logger;
    }

    @Override
    public ContextId getContextId() {
        return contextId;
    }

    public int getGlobalParallelFactor() {
        return globalParallelFactor;
    }

    public GpfdistMetadata getGpfdistMetadata(String processorTaskId) {
        return Optional.ofNullable(metadataMap.get(processorTaskId))
                .orElseThrow(() -> new IllegalArgumentException("No metadata found for processor task id " + processorTaskId));
    }

    public RecordProcessingService getRecordProcessingService(String processorTaskId) {
        return Optional.ofNullable(recordProcessingServiceMap.get(processorTaskId))
                .orElseThrow(() -> new IllegalArgumentException("No record processing service for processor task id " + processorTaskId));
    }

    public Map<String, CompletableFuture<Void>> getUnloadQueryFutureMap() {
        return unloadQueryFutureMap;
    }

    public RecordSchema getRecordSchema() {
        return recordSchema;
    }

    public Map<String, ColumnDataType> getDataTypes() {
        return dataTypes;
    }

    public ComponentLog getLogger() {
        return logger;
    }

    @Override
    public String toString() {
        return "ReadContext{" +
                "contextId=" + contextId +
                ", globalParallelFactor=" + globalParallelFactor +
                ", recordSchema=" + recordSchema +
                ", dataTypes=" + dataTypes +
                ", metadataMap=" + metadataMap +
                ", recordProcessingServiceMap=" + recordProcessingServiceMap +
                '}';
    }

    @Override
    public void close() {
        recordProcessingServiceMap.values().forEach(rps -> {
            rps.stop();
            rps.clear();
        });
        unloadQueryFutureMap.forEach((taskId, unloadQueryFutures) -> {
            if (!unloadQueryFutures.isDone()) {
                try {
                    unloadQueryFutures.completeExceptionally(new RuntimeException("Unloading was stopped"));
                } catch (Exception e) {
                    logger.warn("Failed to stop unloading query future for taskId: {}", taskId, e);
                }
            }
        });
        metadataMap.clear();
        recordProcessingServiceMap.clear();
        unloadQueryFutureMap.clear();
        logger.info("Closed read context with id: {}", contextId);
    }
}
