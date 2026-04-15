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
import org.apache.nifi.gpfdist.service.CancellableQuery;
import org.apache.nifi.gpfdist.service.TransferDataQueryExecutor;
import org.apache.nifi.gpfdist.service.unload.process.RecordProcessingService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class ReadContext implements Context {
    private static final long RETRY_DELAY_BASE_MILLIS = 1_000L;
    private static final int RETRY_DELAY_MAX_EXPONENT = 5;
    private static final long RETRY_DELAY_MAX_MILLIS = 30_000L;

    private final ContextId contextId;
    private final int globalParallelFactor;
    private final RecordSchema recordSchema;
    private final Map<String, ColumnDataType> dataTypes;
    private final Map<String, GpfdistMetadata> metadataMap;
    private final Map<String, RecordProcessingService> recordProcessingServiceMap;
    private final Map<String, CancellableQuery> unloadQueryMap;
    private final Map<String, GreengageTableColumnsMaxValueContext> maxValueTrackingContextMap;
    private final Map<String, Long> retryAfterMap;
    private final Map<String, Integer> failureCountMap;
    private final TransferDataQueryExecutor dropExternalTableQueryExecutor;
    private final ComponentLog logger;

    public ReadContext(ContextId contextId,
                       int globalParallelFactor,
                       RecordSchema recordSchema,
                       Map<String, ColumnDataType> dataTypes,
                       Map<String, GpfdistMetadata> metadataMap,
                       Map<String, RecordProcessingService> recordProcessingServiceMap,
                       TransferDataQueryExecutor dropExternalTableQueryExecutor,
                       ComponentLog logger) {
        this.contextId = contextId;
        this.globalParallelFactor = globalParallelFactor;
        this.recordSchema = recordSchema;
        this.metadataMap = metadataMap;
        this.recordProcessingServiceMap = recordProcessingServiceMap;
        this.dataTypes = dataTypes;
        this.dropExternalTableQueryExecutor = dropExternalTableQueryExecutor;
        unloadQueryMap = new ConcurrentHashMap<>();
        maxValueTrackingContextMap = new ConcurrentHashMap<>();
        retryAfterMap = new ConcurrentHashMap<>();
        failureCountMap = new ConcurrentHashMap<>();
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

    public CompletableFuture<Void> getUnloadQueryFuture(final String taskId) {
        final CancellableQuery unloadQuery = unloadQueryMap.get(taskId);
        return unloadQuery == null ? null : unloadQuery.future();
    }

    public void setUnloadQuery(final String taskId, final CancellableQuery unloadQuery) {
        unloadQueryMap.put(taskId, Objects.requireNonNull(unloadQuery, "unloadQuery cannot be null"));
    }

    public void setTableColumnsMaxValueContext(final String taskId, final GreengageTableColumnsMaxValueContext context) {
        maxValueTrackingContextMap.put(taskId, Objects.requireNonNull(context, "greengageTableColumnsMaxValueContext cannot be null"));
    }

    public Optional<GreengageTableColumnsMaxValueContext> getTableColumnsMaxValueContext(final String taskId) {
        return Optional.ofNullable(maxValueTrackingContextMap.get(taskId));
    }

    public void updateGpfdistMetadata(final String taskId, final GpfdistMetadata metadata) {
        metadataMap.put(taskId, metadata);
    }

    public boolean isTaskInRetryDelay(final String taskId) {
        final Long retryAfter = retryAfterMap.get(taskId);
        return retryAfter != null && retryAfter > System.currentTimeMillis();
    }

    public long getRetryDelayLeftMillis(final String taskId) {
        final Long retryAfter = retryAfterMap.get(taskId);
        if (retryAfter == null) {
            return 0L;
        }
        return Math.max(0L, retryAfter - System.currentTimeMillis());
    }

    public void resetRetryDelay(final String taskId) {
        retryAfterMap.remove(taskId);
        failureCountMap.remove(taskId);
    }

    public void registerTaskFailure(final String taskId) {
        final int failures = failureCountMap.merge(taskId, 1, Integer::sum);
        // exponential backoff with cap:
        // delay = min(RETRY_DELAY_MAX_MILLIS, RETRY_DELAY_BASE_MILLIS * 2^min(RETRY_DELAY_MAX_EXPONENT, failures)).
        // the "retry after" moment is current time plus the computed delay.
        final long retryDelayMillis = Math.min(
                RETRY_DELAY_MAX_MILLIS,
                RETRY_DELAY_BASE_MILLIS * (1L << Math.min(RETRY_DELAY_MAX_EXPONENT, failures))
        );
        retryAfterMap.put(taskId, System.currentTimeMillis() + retryDelayMillis);
    }

    public void clearTaskState(final String taskId) {
        final CancellableQuery unloadQuery = unloadQueryMap.remove(taskId);
        if (unloadQuery != null) {
            try {
                unloadQuery.cancel();
            } catch (Exception e) {
                logger.warn("Failed to cancel unloading query for taskId: {}", taskId, e);
            }
        }
        maxValueTrackingContextMap.remove(taskId);
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
        final List<CompletableFuture<Void>> dropExternalTableFutures = new ArrayList<>();
        for (GpfdistMetadata metadata : metadataMap.values()) {
            try {
                dropExternalTableFutures.add(dropExternalTableQueryExecutor.execute(metadata));
            } catch (Exception e) {
                logger.warn("Failed to submit drop external table task for metadata {}", metadata, e);
            }
        }
        try {
            CompletableFuture.allOf(dropExternalTableFutures.toArray(new CompletableFuture[0])).join();
            recordProcessingServiceMap.values().forEach(rps -> {
                rps.stop();
                rps.clear();
            });
            unloadQueryMap.forEach((taskId, unloadQuery) -> {
                try {
                    unloadQuery.cancel();
                } catch (Exception e) {
                    logger.warn("Failed to stop unloading query for taskId: {}", taskId, e);
                }
            });
        } catch (Exception e) {
            logger.warn("Context were not close successfully for id: {}", contextId, e);
        } finally {
            metadataMap.clear();
            recordProcessingServiceMap.clear();
            unloadQueryMap.clear();
            maxValueTrackingContextMap.clear();
            retryAfterMap.clear();
            failureCountMap.clear();
            logger.info("Closed read context with id: {}", contextId);
        }
    }
}
