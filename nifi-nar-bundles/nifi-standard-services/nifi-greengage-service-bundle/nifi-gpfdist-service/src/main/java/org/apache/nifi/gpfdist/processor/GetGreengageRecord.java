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

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.metadata.TableDescription;
import org.apache.nifi.gpfdist.service.GpfdistService;
import org.apache.nifi.gpfdist.service.GpfdistUnloadMetadataFactory;
import org.apache.nifi.gpfdist.service.TransferDataQueryExecutor;
import org.apache.nifi.gpfdist.service.context.GpfdistContextId;
import org.apache.nifi.gpfdist.service.datatype.GreenplumColumnDataTypeConverter;
import org.apache.nifi.gpfdist.service.unload.context.ReadContext;
import org.apache.nifi.gpfdist.service.unload.context.ReadContextManager;
import org.apache.nifi.gpfdist.service.unload.dto.ProcessorTaskResult;
import org.apache.nifi.gpfdist.service.unload.dto.UnloadingResult;
import org.apache.nifi.gpfdist.service.unload.process.FlowFileGenerator;
import org.apache.nifi.gpfdist.service.unload.process.GpfdistRecordProcessingService;
import org.apache.nifi.gpfdist.service.unload.process.ProcessorTaskManager;
import org.apache.nifi.gpfdist.service.unload.process.RecordProcessingService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.QUOTE;

@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
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

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from input records.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if records cannot be loaded into Greengage.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);
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
            MAX_FLOWFILES_PER_TRIGGER);

    private ProcessorTaskManager processorTaskManager;
    private TransferDataQueryExecutor transferDataQueryExecutor;
    private ReadContext readContext;
    private FlowFileGenerator flowFileGenerator;
    private ReadContextManager readContextManager;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        int parallelFactor = context.getProperty(NODE_PARALLEL_FACTOR).asInteger();
        int batchRecordCount = context.getProperty(READ_BATCH_RECORD_COUNT).evaluateAttributeExpressions().asInteger();
        int maxRecordsBufferSize = context.getProperty(RECORD_BUFFER_SIZE).evaluateAttributeExpressions().asInteger();
        int pullBufferedRecordsTimeoutMs = context.getProperty(PULL_BUFFERED_RECORDS_TIMEOUT_MS).evaluateAttributeExpressions().asInteger();
        int maxFlowFilesPerTrigger = context.getProperty(MAX_FLOWFILES_PER_TRIGGER).asInteger();

        GpfdistService gpfdistService = context.getProperty(GPFDIST_SERVICE).asControllerService(GpfdistService.class);
        String schema = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions().getValue();
        String table = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        String columns = context.getProperty(TABLE_COLUMNS).evaluateAttributeExpressions().getValue();
        RecordSetWriterFactory recordSetWriterFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        readContextManager = (ReadContextManager) gpfdistService.getReadContextManager();
        TableDescription tableDescription = gpfdistService.getGreengageMetadataService().getTableDescription(schema, table);
        transferDataQueryExecutor = gpfdistService.getUnloadDataQueryExecutor();
        GpfdistUnloadMetadataFactory gpfdistUnloadMetadataFactory = gpfdistService.getGpfdistUnloadMetadataFactory();
        List<ColumnDescription> columnDescriptions = getColumnDescriptions(columns, tableDescription);
        RecordSchema recordSchema = GreenplumColumnDataTypeConverter.convert(columnDescriptions);
        Map<String, ColumnDataType> dataTypes = columnDescriptions.stream()
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
                    columnDescriptions,
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
            GpfdistMetadata gpfdistMetadata = readContext.getGpfdistMetadata(processorTaskId);
            RecordProcessingService recordService = readContext.getRecordProcessingService(processorTaskId);
            CompletableFuture<Void> unloadFuture = readContext.getUnloadQueryFutureMap()
                    .computeIfAbsent(processorTaskId, taskId -> transferDataQueryExecutor.execute(gpfdistMetadata));
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
                    context.yield();
                    processorTaskManager.remove(processorTask);
                    getLogger().error("Unloading query failed for task: {}; Error: {}", processorTaskId, errorMsg);
                    return;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    session.rollback();
                    context.yield();
                    processorTaskManager.remove(processorTask);
                    getLogger().error("Unloading failed for task: {}; Error: {}", processorTaskId, ie.getMessage());
                    return;
                }
                List<FlowFile> last = flowFileGenerator.createFlowFiles(session, processorTaskId, recordService);
                transferFlowFiles(session, last);

                if (recordService.isFinished()) {
                    processorTaskManager.remove(processorTask);
                    ProcessorTaskResult result = ProcessorTaskResult.aggregate(processorTaskId, recordService.getResult());
                    getLogger().info("Unloading data for task: {} is fully completed. Result: {}. Task is removed from queue",
                            processorTaskId, result);
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
            context.yield();
            processorTaskManager.remove(processorTask);
            getLogger().error("Unexpected error in processor for task: {}; Error: {}", processorTaskId, e.getMessage(), e);
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
}
