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
package org.apache.nifi.gpfdist.processor;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextManager;
import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.metadata.GreengageDataType;
import org.apache.nifi.gpfdist.metadata.TableDescription;
import org.apache.nifi.gpfdist.service.CancellableQuery;
import org.apache.nifi.gpfdist.service.GpfdistService;
import org.apache.nifi.gpfdist.service.GpfdistUnloadMetadataFactory;
import org.apache.nifi.gpfdist.service.TransferDataQueryExecutor;
import org.apache.nifi.gpfdist.service.context.GpfdistContextId;
import org.apache.nifi.gpfdist.service.unload.context.GreengageTableColumnsMaxValueContext;
import org.apache.nifi.gpfdist.service.unload.context.ReadContext;
import org.apache.nifi.gpfdist.service.unload.dto.ProcessorTaskResult;
import org.apache.nifi.gpfdist.service.unload.dto.UnloadingResult;
import org.apache.nifi.gpfdist.service.unload.metadata.GpfdistUnloadMetadata;
import org.apache.nifi.gpfdist.service.unload.process.FlowFileGenerator;
import org.apache.nifi.gpfdist.service.unload.process.GpfdistRecordProcessingService;
import org.apache.nifi.gpfdist.service.unload.process.ProcessorTaskManager;
import org.apache.nifi.gpfdist.service.unload.process.RecordProcessingService;
import org.apache.nifi.gpfdist.service.util.GreengageColumnDataTypeConverter;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.QUOTE;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.compareByType;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.getStateKey;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.getQualifiedName;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.SUPPORTED_MAX_VALUE_TYPES;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.SUPPORTED_MAX_VALUE_TYPES_DESCRIPTION;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Stateful(scopes = Scope.CLUSTER, description = "Stores maximum observed values per worker and column for incremental unloading from Greengage.")
@Tags({"record", "get", "greengage"})
@CapabilityDescription("Read records from Greengage to FlowFiles")
public class GetGreengageRecord extends AbstractProcessor {
    static final PropertyDescriptor GPFDIST_SERVICE = new PropertyDescriptor.Builder()
            .name("gpfdist-record-processing-service")
            .displayName("Gpfdist Service")
            .description("The Controller Service that is used to load records into greengage.")
            .required(true)
            .identifiesControllerService(GpfdistService.class)
            .build();
    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("get-greengage-record-schema-name")
            .displayName("Schema Name")
            .description("The name of the schema where the data will be loaded.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("get-greengage-record-table-name")
            .displayName("Table Name")
            .description("Name of the table where the data will be loaded.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor TABLE_COLUMNS = new PropertyDescriptor.Builder()
            .name("get-greengage-table-columns")
            .displayName("Table Columns")
            .description("Columns of the table where the data will be loaded.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("The RecordSetWriterFactory that will be used for writing records.")
            .required(true)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();
    static final PropertyDescriptor NODE_PARALLEL_FACTOR = new PropertyDescriptor.Builder()
            .name("node-parallel-factor")
            .displayName("Node Parallel Factor")
            .description("Number of parallel tasks/external tables for unloading data per node.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();
    static final PropertyDescriptor READ_BATCH_RECORD_COUNT = new PropertyDescriptor.Builder()
            .name("Read Batch Record Count")
            .displayName("Read Batch Record Count")
            .description("The batch count of records to read from the buffer queue to generate the flow file.")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("10000")
            .build();
    static final PropertyDescriptor RECORD_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("record-buffer-size-per-task")
            .displayName("Record Buffer Size Per Task")
            .description("Buffer size of processed parsed input csv records per task.")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("10000")
            .build();
    static final PropertyDescriptor PULL_BUFFERED_RECORDS_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("pull-buffered-records-timeout-ms")
            .displayName("Pull Buffer Records Timeout")
            .description("Timeout for polling records from the buffer queue in milliseconds.")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();
    static final PropertyDescriptor MAX_FLOWFILES_PER_TRIGGER = new PropertyDescriptor.Builder()
            .name("max-flowfiles-per-thread-call")
            .displayName("Max FlowFiles Per Trigger")
            .description("The maximum number of flow files to create in one call to the parallel nifi thread unload method.")
            .required(false)
            .defaultValue("10")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor MAX_VALUE_COLUMN_NAMES = new PropertyDescriptor.Builder()
            .name("Maximum-value Columns")
            .displayName("Maximum-value Columns Names")
            .description("Optional. A comma-separated list of column names. When configured, the processor will keep track of the maximum value "
                    + "for each column that has been returned since the processor started running. Using multiple columns implies an order "
                    + "to the column list; column tuples are compared lexicographically in that order. This processor "
                    + "can be used to retrieve only those rows that have been added/updated since the last retrieval. Columns listed in this property "
                    + "must be NOT NULL. Supported types: " + SUPPORTED_MAX_VALUE_TYPES_DESCRIPTION + ". If no columns "
                    + "are provided, the processor performs full table unloading once for each worker task, "
                    + "and then skips further unload cycles until processor state is cleared. NOTE: It is important "
                    + "to use consistent max-value column names for a given table for incremental fetch to work properly.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from input records.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if records cannot be loaded into Greengage.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);
    private static final int MAX_STATE_UPDATE_ATTEMPTS = 10;
    private static final String FULL_LOAD_DONE_STATE_KEY_SUFFIX = "full_load_done";
    private static final String FULL_LOAD_DONE_STATE_VALUE = "true";
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GPFDIST_SERVICE,
            SCHEMA_NAME,
            TABLE_NAME,
            TABLE_COLUMNS,
            RECORD_WRITER,
            NODE_PARALLEL_FACTOR,
            READ_BATCH_RECORD_COUNT,
            RECORD_BUFFER_SIZE,
            PULL_BUFFERED_RECORDS_TIMEOUT_MS,
            MAX_FLOWFILES_PER_TRIGGER,
            MAX_VALUE_COLUMN_NAMES);

    private ProcessorTaskManager processorTaskManager;
    private TransferDataQueryExecutor insertFromExternalTableQueryExecutor;
    private TransferDataQueryExecutor createWriteExternalTableQueryExecutor;
    private ReadContext readContext;
    private FlowFileGenerator flowFileGenerator;
    private ContextManager<Context> readContextManager;
    private GpfdistService gpfdistService;
    private GpfdistUnloadMetadataFactory gpfdistUnloadMetadataFactory;
    private TableDescription tableDescription;
    private List<ColumnDescription> selectedColumns;
    private List<String> maxValueColumnNamesList = Collections.emptyList();
    private Map<String, ColumnDataType> maxValueColumnTypes = Collections.emptyMap();
    private String stateTablePrefix;
    private final Map<Integer, Map<String, String>> pendingLowerValuesByWorker = new ConcurrentHashMap<>();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @SuppressWarnings("unchecked")
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        pendingLowerValuesByWorker.clear();
        int parallelFactor = context.getProperty(NODE_PARALLEL_FACTOR).asInteger();
        int batchRecordCount = context.getProperty(READ_BATCH_RECORD_COUNT).evaluateAttributeExpressions().asInteger();
        int maxRecordsBufferSize = context.getProperty(RECORD_BUFFER_SIZE).evaluateAttributeExpressions().asInteger();
        int pullBufferedRecordsTimeoutMs = context.getProperty(PULL_BUFFERED_RECORDS_TIMEOUT_MS).evaluateAttributeExpressions().asInteger();
        int maxFlowFilesPerTrigger = context.getProperty(MAX_FLOWFILES_PER_TRIGGER).asInteger();
        String maxValueColumnNames = context.getProperty(MAX_VALUE_COLUMN_NAMES).evaluateAttributeExpressions().getValue();

        gpfdistService = context.getProperty(GPFDIST_SERVICE).asControllerService(GpfdistService.class);
        String schema = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions().getValue();
        String table = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        String columns = context.getProperty(TABLE_COLUMNS).evaluateAttributeExpressions().getValue();
        RecordSetWriterFactory recordSetWriterFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        readContextManager = (ContextManager<Context>) gpfdistService.getReadContextManager();
        tableDescription = gpfdistService.getGreengageMetadataService().getTableDescription(schema, table);
        stateTablePrefix = getQualifiedName(schema, table);
        createWriteExternalTableQueryExecutor = gpfdistService.getCreateWriteExternalTableQueryExecutor();
        insertFromExternalTableQueryExecutor = gpfdistService.getInsertDataFromTargetTableQueryExecutor();
        final TransferDataQueryExecutor dropExternalTableQueryExecutor = gpfdistService.getDropExternalTableQueryExecutor();
        gpfdistUnloadMetadataFactory = gpfdistService.getGpfdistUnloadMetadataFactory();
        selectedColumns = getColumnDescriptions(columns, tableDescription);
        maxValueColumnNamesList = parseColumnNames(maxValueColumnNames);
        maxValueColumnTypes = getMaxValueColumnTypes(maxValueColumnNamesList, tableDescription);
        RecordSchema recordSchema = GreengageColumnDataTypeConverter.convert(selectedColumns);
        Map<String, ColumnDataType> dataTypes = selectedColumns.stream()
                .collect(Collectors.toMap(ColumnDescription::getName, ColumnDescription::getDataType));

        int segmentCount = gpfdistService.getGreengageMetadataService().getSegmentCount();
        int nifiThreads = context.getMaxConcurrentTasks();
        int nodeIndex = getNodeIndex(gpfdistService);
        int nodeCount = gpfdistService.getNodeIndexService().getNodeCount();

        processorTaskManager = new ProcessorTaskManager(nodeIndex,
                nodeCount,
                parallelFactor,
                segmentCount,
                getLogger());

        getLogger().info("Processing task thread count: {}", nifiThreads);
        GpfdistContextId contextId = new GpfdistContextId(UUID.randomUUID().toString());
        Map<String, GpfdistMetadata> metadataMap = new ConcurrentHashMap<>();
        Map<String, RecordProcessingService> recordProcessingServiceMap = new ConcurrentHashMap<>();

        processorTaskManager.getProcessorTask().forEach(processorTask -> {
            metadataMap.put(processorTask.getId(), gpfdistUnloadMetadataFactory.create(tableDescription,
                    selectedColumns,
                    contextId,
                    processorTask.getId(),
                    processorTask.getGlobalWorkerIndex()));
            recordProcessingServiceMap.put(processorTask.getId(),
                    new GpfdistRecordProcessingService(processorTask.getId(), maxRecordsBufferSize, getLogger()));
        });
        readContext = new ReadContext(contextId,
                processorTaskManager.getGlobalParallelFactor(),
                recordSchema,
                dataTypes,
                metadataMap,
                recordProcessingServiceMap,
                dropExternalTableQueryExecutor,
                getLogger());
        flowFileGenerator = new FlowFileGenerator(recordSetWriterFactory,
                recordSchema,
                batchRecordCount,
                pullBufferedRecordsTimeoutMs,
                maxFlowFilesPerTrigger,
                getLogger());
        getLogger().info("Created FlowFile generator: {}", flowFileGenerator);
        readContextManager.add(readContext);
        getLogger().info("Created read context: {}", readContext);
    }

    private int getNodeIndex(GpfdistService gpfdistService) {
        try {
            return gpfdistService.getNodeIndexService().getNodeIndex();
        } catch (Exception e) {
            getLogger().error("Failed to get node index", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ProcessorTaskManager.ProcessorTask processorTask = processorTaskManager.acquire();
        if (processorTask == null) {
            context.yield();
            return;
        }
        getLogger().debug("Acquired task: {}", processorTask);
        final String processorTaskId = processorTask.getId();
        try {
            GpfdistUnloadMetadata gpfdistMetadata = (GpfdistUnloadMetadata) readContext.getGpfdistMetadata(processorTaskId);
            RecordProcessingService recordService = readContext.getRecordProcessingService(processorTaskId);
            if (readContext.isTaskInRetryDelay(processorTaskId)) {
                getLogger().debug("Task {} is in retryDelay for {} ms", processorTaskId, readContext.getRetryDelayLeftMillis(processorTaskId));
                processorTaskManager.release(processorTask);
                context.yield();
                return;
            }

            CompletableFuture<Void> unloadFuture = readContext.getUnloadQueryFuture(processorTaskId);
            if (unloadFuture == null) {
                unloadFuture = createUnloadFuture(session, processorTask, gpfdistMetadata);
                if (unloadFuture == null) {
                    processorTaskManager.release(processorTask);
                    context.yield();
                    return;
                }
            }
            List<FlowFile> readyFlowFiles = flowFileGenerator.createFlowFiles(session, processorTaskId, recordService);

            if (readyFlowFiles.isEmpty() && !unloadFuture.isDone()) {
                processorTaskManager.release(processorTask);
                context.yield();
                return;
            }
            transferFlowFiles(session, readyFlowFiles);
            if (unloadFuture.isDone()) {
                try {
                    unloadFuture.get();
                    getLogger().info("Unloading greengage query is completed for task: {}", processorTaskId);
                } catch (ExecutionException ee) {
                    Throwable cause = ee.getCause();
                    String errorMsg;
                    List<UnloadingResult> failedResults = recordService.getResult().stream()
                            .filter(r -> r.getError() != null)
                            .collect(Collectors.toList());
                    if (!failedResults.isEmpty()) {
                        errorMsg = failedResults.stream()
                                .map(r -> String.format("Failed to unload data for chunkId: %s, error: %s",
                                        r.getChunkId(),
                                        r.getError().getMessage()))
                                .collect(Collectors.joining(";"));
                    } else {
                        errorMsg = cause.getMessage();
                    }
                    session.rollback();
                    readContext.registerTaskFailure(processorTaskId);
                    cleanupTaskCycle(processorTask, gpfdistMetadata, recordService, false);
                    context.yield();
                    getLogger().error("Unloading query failed for task: {}; Error: {}", processorTaskId, errorMsg);
                    processorTaskManager.release(processorTask);
                    return;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    session.rollback();
                    readContext.registerTaskFailure(processorTaskId);
                    cleanupTaskCycle(processorTask, gpfdistMetadata, recordService, false);
                    context.yield();
                    getLogger().error("Unloading failed for task: {}; Error: {}", processorTaskId, ie.getMessage());
                    processorTaskManager.release(processorTask);
                    return;
                }
                List<FlowFile> last = flowFileGenerator.createFlowFiles(session, processorTaskId, recordService);
                transferFlowFiles(session, last);

                if (recordService.isFinished()) {
                    final ProcessorTaskResult result = ProcessorTaskResult.aggregate(
                            processorTaskId,
                            new ArrayList<>(recordService.getResult()));
                    updateState(session, processorTask);
                    cleanupTaskCycle(processorTask, gpfdistMetadata, recordService, true);
                    getLogger().info("Unloading data for task: {} is fully completed. Result: {}. Task is released for next cycle",
                            processorTaskId, result);
                    processorTaskManager.release(processorTask);
                } else {
                    processorTaskManager.release(processorTask);
                    getLogger().debug("Task: {} still has active segments or records; task slot released", processorTaskId);
                }
            } else {
                processorTaskManager.release(processorTask);
                getLogger().debug("Unloading still in progress for task: {}. Slot released", processorTaskId);
            }
        } catch (Exception e) {
            session.rollback();
            readContext.registerTaskFailure(processorTaskId);
            try {
                final GpfdistUnloadMetadata metadata = (GpfdistUnloadMetadata) readContext.getGpfdistMetadata(processorTaskId);
                final RecordProcessingService recordService = readContext.getRecordProcessingService(processorTaskId);
                cleanupTaskCycle(processorTask, metadata, recordService, false);
            } catch (Exception cleanupEx) {
                getLogger().warn("Failed to cleanup task {} after error", processorTaskId, cleanupEx);
            }
            context.yield();
            getLogger().error("Unexpected error in processor for task: {}; Error: {}", processorTaskId, e.getMessage(), e);
            processorTaskManager.release(processorTask);
        }
    }

    private CompletableFuture<Void> createUnloadFuture(final ProcessSession session,
                                                       final ProcessorTaskManager.ProcessorTask processorTask,
                                                       final GpfdistUnloadMetadata metadata) throws IOException {
        if (maxValueColumnNamesList.isEmpty() && isFullLoadDone(session, processorTask.getGlobalWorkerIndex())) {
            getLogger().debug("Skipping unload for task: {} as full load is already done", processorTask.getId());
            return null;
        }

        final int workerIndex = processorTask.getGlobalWorkerIndex();
        final Map<String, String> lowerValues = getEffectiveLowerValues(session, workerIndex);
        final Map<String, String> upperValues = getWorkerMaxValues(workerIndex);
        if (!hasNewData(lowerValues, upperValues)) {
            return null;
        }
        if (!maxValueColumnNamesList.isEmpty()) {
            readContext.setTableColumnsMaxValueContext(processorTask.getId(), new GreengageTableColumnsMaxValueContext(
                    maxValueColumnNamesList,
                    maxValueColumnTypes,
                    lowerValues,
                    upperValues));
        }
        final CancellableQuery unloadQuery = executeUnloadQuery(metadata);
        final CompletableFuture<Void> unloadFuture = unloadQuery.future();
        readContext.setUnloadQuery(processorTask.getId(), unloadQuery);
        return unloadFuture;
    }

    private Map<String, String> getCurrentStateWorkerValues(final ProcessSession session, final int workerIndex) throws IOException {
        return getCurrentStateWorkerValues(session.getState(Scope.CLUSTER), workerIndex);
    }

    private Map<String, String> getCurrentStateWorkerValues(final StateMap stateMap, final int workerIndex) {
        final Map<String, String> values = new LinkedHashMap<>();
        for (String column : maxValueColumnNamesList) {
            final String stateKey = getStateKey(stateTablePrefix, workerIndex, column);
            final String value = stateMap.get(stateKey);
            if (value != null) {
                values.put(column, value);
            }
        }
        return values;
    }

    private Map<String, String> getWorkerMaxValues(final int workerIndex) {
        if (maxValueColumnNamesList.isEmpty()) {
            return Collections.emptyMap();
        }
        return gpfdistService.getGreengageMetadataService()
                .getTableColumnsUpperBoundTuples(tableDescription.getSchemaName(),
                        tableDescription.getTableName(),
                        maxValueColumnNamesList,
                        readContext.getGlobalParallelFactor(),
                        workerIndex);
    }

    private boolean hasNewData(final Map<String, String> lowerValues, final Map<String, String> upperValues) {
        if (maxValueColumnNamesList.isEmpty()) {
            return true;
        }
        if (!hasAllValues(upperValues)) {
            return false;
        }
        if (!hasAllValues(lowerValues)) {
            return true;
        }
        return compareTuples(lowerValues, upperValues) < 0;
    }

    private boolean hasAllValues(final Map<String, String> values) {
        return maxValueColumnNamesList.stream().allMatch(column -> values.get(column) != null);
    }

    private int compareTuples(final Map<String, String> leftValues, final Map<String, String> rightValues) {
        for (String column : maxValueColumnNamesList) {
            final int compared = compareByType(maxValueColumnTypes, column, leftValues.get(column), rightValues.get(column));
            if (compared != 0) {
                return compared;
            }
        }
        return 0;
    }

    private boolean isFullLoadDone(final ProcessSession session, final int workerIndex) throws IOException {
        final String stateKey = getStateKey(stateTablePrefix, workerIndex, FULL_LOAD_DONE_STATE_KEY_SUFFIX);
        return FULL_LOAD_DONE_STATE_VALUE.equalsIgnoreCase(session.getState(Scope.CLUSTER).get(stateKey));
    }

    private void updateState(final ProcessSession session, final ProcessorTaskManager.ProcessorTask processorTask) throws IOException {
        if (maxValueColumnNamesList.isEmpty()) {
            markFullLoadDone(session, processorTask.getGlobalWorkerIndex());
            return;
        }

        final Map<String, String> upperValues = readContext.getTableColumnsMaxValueContext(processorTask.getId())
                .map(GreengageTableColumnsMaxValueContext::getUpperValues)
                .orElse(Collections.emptyMap());
        if (upperValues.isEmpty()) {
            return;
        }
        updateClusterStateWithRetry(
                session,
                processorTask.getGlobalWorkerIndex(),
                "Failed to update max values state after %d attempts for worker %d",
                "Failed to update max values state for worker {}",
                (stateMap, updatedState) -> {
                    final Map<String, String> currentValues = getCurrentStateWorkerValues(stateMap, processorTask.getGlobalWorkerIndex());
                    if (hasAllValues(currentValues) && hasAllValues(upperValues)
                            && compareTuples(currentValues, upperValues) >= 0) {
                        return false;
                    }
                    upperValues.forEach((columnName, value) -> {
                        if (value != null) {
                            updatedState.put(getStateKey(stateTablePrefix, processorTask.getGlobalWorkerIndex(), columnName), value);
                        }
                    });
                    return true;
                    });
        pendingLowerValuesByWorker.put(processorTask.getGlobalWorkerIndex(), new LinkedHashMap<>(upperValues));
    }

    private void markFullLoadDone(final ProcessSession session, final int workerIndex) throws IOException {
        final String stateKey = getStateKey(stateTablePrefix, workerIndex, FULL_LOAD_DONE_STATE_KEY_SUFFIX);
        updateClusterStateWithRetry(
                session,
                workerIndex,
                "Failed to update full load state after %d attempts for worker %d",
                "Failed to mark full load done state for worker {}",
                (stateMap, updatedState) -> {
                    if (FULL_LOAD_DONE_STATE_VALUE.equalsIgnoreCase(stateMap.get(stateKey))) {
                        return false;
                    }
                    updatedState.put(stateKey, FULL_LOAD_DONE_STATE_VALUE);
                    return true;
                });
    }

    private void updateClusterStateWithRetry(final ProcessSession session,
                                             final int workerIndex,
                                             final String exhaustedAttemptsMessage,
                                             final String errorMessage,
                                             final BiFunction<StateMap, Map<String, String>, Boolean> stateUpdater) throws IOException {
        try {
            for (int attempt = 1; attempt <= MAX_STATE_UPDATE_ATTEMPTS; attempt++) {
                final StateMap stateMap = session.getState(Scope.CLUSTER);
                final Map<String, String> updatedState = new HashMap<>(stateMap.toMap());
                final boolean updateRequired = stateUpdater.apply(stateMap, updatedState);
                if (!updateRequired) {
                    return;
                }

                if (session.replaceState(stateMap, updatedState, Scope.CLUSTER)) {
                    return;
                }
            }

            throw new IllegalStateException(String.format(exhaustedAttemptsMessage, MAX_STATE_UPDATE_ATTEMPTS, workerIndex));
        } catch (IOException e) {
            getLogger().error(errorMessage, workerIndex, e);
            throw e;
        }
    }

    private CancellableQuery executeUnloadQuery(final GpfdistUnloadMetadata metadata) {
        final AtomicReference<CancellableQuery> activeQueryRef = new AtomicReference<>();
        final AtomicBoolean canceled = new AtomicBoolean(false);
        final CompletableFuture<Void> unloadFuture = new CompletableFuture<>();

        final CancellableQuery createExternalTableQuery = createWriteExternalTableQueryExecutor.executeCancellable(metadata);
        activeQueryRef.set(createExternalTableQuery);
        createExternalTableQuery.future().whenComplete((v, createError) -> {
            if (createError != null) {
                unloadFuture.completeExceptionally(createError);
                return;
            }
            if (canceled.get()) {
                unloadFuture.cancel(true);
                return;
            }

            final CancellableQuery insertFromExternalTableQuery = insertFromExternalTableQueryExecutor.executeCancellable(metadata);
            activeQueryRef.set(insertFromExternalTableQuery);
            if (canceled.get()) {
                insertFromExternalTableQuery.cancel();
                unloadFuture.cancel(true);
                return;
            }

            insertFromExternalTableQuery.future().whenComplete((insertV, insertError) -> {
                if (insertError != null) {
                    unloadFuture.completeExceptionally(insertError);
                } else {
                    unloadFuture.complete(null);
                }
            });
        });

        return new CancellableQuery() {
            @Override
            public CompletableFuture<Void> future() {
                return unloadFuture;
            }

            @Override
            public void cancel() {
                canceled.set(true);
                final CancellableQuery activeQuery = activeQueryRef.get();
                if (activeQuery != null) {
                    activeQuery.cancel();
                }
                unloadFuture.cancel(true);
            }
        };
    }

    private void cleanupTaskCycle(final ProcessorTaskManager.ProcessorTask processorTask,
                                  final GpfdistUnloadMetadata gpfdistMetadata,
                                  final RecordProcessingService recordService,
                                  final boolean resetRetryDelay) {
        clearTaskState(processorTask);
        dropExternalTable(gpfdistMetadata);
        resetRecordProcessingService(processorTask, recordService);
        readContext.updateGpfdistMetadata(processorTask.getId(), gpfdistUnloadMetadataFactory.create(tableDescription,
                selectedColumns,
                readContext.getContextId(),
                processorTask.getId(),
                processorTask.getGlobalWorkerIndex()));
        if (resetRetryDelay) {
            readContext.resetRetryDelay(processorTask.getId());
        }
    }

    private void clearTaskState(ProcessorTaskManager.ProcessorTask processorTask) {
        try {
            readContext.clearTaskState(processorTask.getId());
        } catch (Exception e) {
            getLogger().warn("Failed to clear runtime state for task {}", processorTask.getId(), e);
        }
    }

    private void dropExternalTable(GpfdistUnloadMetadata gpfdistMetadata) {
        try {
            gpfdistService.getDropExternalTableQueryExecutor().execute(gpfdistMetadata).get();
        } catch (Exception e) {
            getLogger().warn("Failed to drop external table for task {}", gpfdistMetadata.getProcessorTaskId(), e);
        }
    }

    private void resetRecordProcessingService(ProcessorTaskManager.ProcessorTask processorTask, RecordProcessingService recordService) {
        try {
            recordService.resetForNextCycle();
        } catch (Exception e) {
            getLogger().warn("Failed to reset record processing service for task {}", processorTask.getId(), e);
        }
    }

    private void transferFlowFiles(ProcessSession session, List<FlowFile> ready) {
        for (FlowFile ff : ready) {
            session.transfer(ff, REL_SUCCESS);
        }
    }

    @OnUnscheduled
    public void onUnschedule(final ProcessContext context) {
        try {
            if (readContext != null && readContextManager != null) {
                readContextManager.remove(readContext.getContextId());
                readContext.close();
            }
        } catch (Exception e) {
            getLogger().warn("Failed to close read context {}", readContext.getContextId(), e);
        }
        pendingLowerValuesByWorker.clear();
    }

    private Map<String, String> getEffectiveLowerValues(final ProcessSession session, final int workerIndex) throws IOException {
        final Map<String, String> stateLowerValues = getCurrentStateWorkerValues(session, workerIndex);
        if (maxValueColumnNamesList.isEmpty() || hasAllValues(stateLowerValues)) {
            pendingLowerValuesByWorker.remove(workerIndex);
            return stateLowerValues;
        }
        final Map<String, String> pendingLowerValues = pendingLowerValuesByWorker.remove(workerIndex);
        if (pendingLowerValues != null && hasAllValues(pendingLowerValues)) {
            getLogger().debug("Using pending lower values for worker {} before cluster state is visible: {}", workerIndex, pendingLowerValues);
            return pendingLowerValues;
        }
        return stateLowerValues;
    }

    private List<ColumnDescription> getColumnDescriptions(String columnsProperty,
                                                          TableDescription tableDescription) {
        List<String> columns = Arrays.stream(columnsProperty.split(","))
                .map(col -> col.replace(QUOTE, "").trim())
                .collect(Collectors.toList());
        List<ColumnDescription> columnDescriptions = new ArrayList<>();
        for (String s : columns) {
            ColumnDescription columnDescription = tableDescription.getColumns().get(s);
            if (columnDescription == null) {
                throw new IllegalStateException("Column " + s + " not found in table " + tableDescription.getTableName());
            }
            columnDescriptions.add(columnDescription);
        }
        return columnDescriptions;
    }

    private List<String> parseColumnNames(final String columnsProperty) {
        if (columnsProperty == null || columnsProperty.isBlank()) {
            return Collections.emptyList();
        }
        return Arrays.stream(columnsProperty.split(","))
                .map(col -> col.replace(QUOTE, "").trim())
                .filter(col -> !col.isEmpty())
                .collect(Collectors.toList());
    }

    private Map<String, ColumnDataType> getMaxValueColumnTypes(final List<String> maxColumns,
                                                               final TableDescription tableDescription) {
        final Map<String, ColumnDataType> result = new LinkedHashMap<>();
        for (String maxColumn : maxColumns) {
            final ColumnDescription columnDescription = tableDescription.getColumns().get(maxColumn);
            if (columnDescription == null) {
                throw new IllegalStateException("Column " + maxColumn + " not found in table " + tableDescription.getTableName());
            }
            if (columnDescription.isNullable()) {
                throw new IllegalStateException("Column " + maxColumn + " must be NOT NULL for max value tracking");
            }
            final GreengageDataType type = columnDescription.getDataType().getType();
            if (!SUPPORTED_MAX_VALUE_TYPES.contains(type)) {
                throw new IllegalStateException("Column " + maxColumn + " has unsupported type for max value tracking: " + type
                        + ". Supported types: " + SUPPORTED_MAX_VALUE_TYPES_DESCRIPTION);
            }
            result.put(maxColumn, columnDescription.getDataType());
        }
        return result;
    }
}
