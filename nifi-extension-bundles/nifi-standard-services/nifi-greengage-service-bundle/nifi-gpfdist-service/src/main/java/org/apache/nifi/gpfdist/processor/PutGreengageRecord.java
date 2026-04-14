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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextManager;
import org.apache.nifi.gpfdist.metadata.TableDescription;
import org.apache.nifi.gpfdist.service.GpfdistService;
import org.apache.nifi.gpfdist.service.GreengageService;
import org.apache.nifi.gpfdist.service.LoadResult;
import org.apache.nifi.gpfdist.service.RecordSink;
import org.apache.nifi.gpfdist.service.TransferDataQueryExecutor;
import org.apache.nifi.gpfdist.service.context.GpfdistContextId;
import org.apache.nifi.gpfdist.service.load.context.WriteContext;
import org.apache.nifi.gpfdist.service.load.metadata.GpfdistLoadMetadata;
import org.apache.nifi.gpfdist.service.load.process.GpfdistRecordSinkManager;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.StopWatch;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.QUOTE;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"record", "put", "greengage"})
@CapabilityDescription("Writes the contents of a FlowFile to Greengage")
public class PutGreengageRecord extends AbstractProcessor {
    static final PropertyDescriptor GPFDIST_SERVICE = new PropertyDescriptor.Builder()
        .name("gpfdist-record-processing-service")
        .displayName("Gpfdist Service")
        .description("The Controller Service that is used to load records into greengage.")
        .required(true)
        .identifiesControllerService(GpfdistService.class)
        .build();
    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
        .name("put-greengage-record-record-reader")
        .displayName("Record Reader")
        .description(
            "Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
        .name("put-greengage-record-schema-name")
        .displayName("Schema Name")
        .description("The name of the schema where the data will be loaded.")
        .required(false)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
        .name("put-greengage-record-table-name")
        .displayName("Table Name")
        .description("Name of the table where the data will be loaded.")
        .required(true)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    static final PropertyDescriptor TABLE_COLUMNS = new PropertyDescriptor.Builder()
        .name("put-greengage-table-columns")
        .displayName("Table Columns")
        .description("Columns of the table where the data will be loaded.")
        .required(true)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    static final PropertyDescriptor GREENGAGE_SEGMENT_CONCURRENCY_MULTIPLIER = new PropertyDescriptor.Builder()
            .name("greengage-segment-concurrency-multiplier")
            .displayName("Greengage Segment Concurrency Multiplier")
            .description("Integer multiplier for Greengage segment parallelism. Typical values: 1 or 2.")
            .required(false)
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor RECORD_PROCESSOR_MAX_THREADS = new PropertyDescriptor.Builder()
            .name("maximum-record-processor-threads")
            .displayName("Maximum Record Processor Threads")
            .description("The maximum amount of threads that are used to process the records")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("8")
            .build();
    static final PropertyDescriptor FLOW_FILE_BATCHING_ENABLED = new PropertyDescriptor.Builder()
            .name("flow-file-batching-enabled")
            .displayName("Flow File Batching Enabled")
            .description("If enabled, processor will group multiple FlowFiles into a single INSERT load based on Batch Size.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    static final PropertyDescriptor FLOW_FILE_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("flow-files-batch-size")
            .displayName("Flow Files Batch Size")
            .description("Maximum total size (bytes) of FlowFiles to include into one load. Soft limit: last FlowFile may exceed it.")
            .required(false)
            .defaultValue("10 MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
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
            RECORD_READER_FACTORY,
            GPFDIST_SERVICE,
            SCHEMA_NAME,
            TABLE_NAME,
            TABLE_COLUMNS,
            GREENGAGE_SEGMENT_CONCURRENCY_MULTIPLIER,
            RECORD_PROCESSOR_MAX_THREADS,
            FLOW_FILE_BATCHING_ENABLED,
            FLOW_FILE_BATCH_SIZE
    );

    private ContextManager<Context> writeContextManager;
    private WriteContext writeContext;
    private GpfdistRecordSinkManager recordSinkManager;

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
        final int recordProcessorMaxThreads = context.getProperty(RECORD_PROCESSOR_MAX_THREADS).asInteger();
        final int segmentConcurrencyMultiplier = context.getProperty(GREENGAGE_SEGMENT_CONCURRENCY_MULTIPLIER).asInteger();
        final GpfdistService gpfdistService = context.getProperty(GPFDIST_SERVICE).asControllerService(GpfdistService.class);
        final int nodeIndex = getNodeIndex(gpfdistService);
        final int nodeCount = gpfdistService.getNodeIndexService().getNodeCount();
        final int segmentCount = gpfdistService.getGreengageMetadataService().getSegmentCount();
        final int concurrentTasksPerNode = context.getMaxConcurrentTasks();
        final TransferDataQueryExecutor insertDataQueryExecutor = gpfdistService.getInsertDataIntoTargetTableQueryExecutor();
        final TransferDataQueryExecutor dropExternalTableQueryExecutor = gpfdistService.getDropExternalTableQueryExecutor();
        final GpfdistContextId contextId = new GpfdistContextId(UUID.randomUUID().toString());

        writeContextManager = (ContextManager<Context>) gpfdistService.getWriteContextManager();
        recordSinkManager = new GpfdistRecordSinkManager(
                segmentCount,
                segmentConcurrencyMultiplier,
                concurrentTasksPerNode,
                nodeCount,
                nodeIndex,
                insertDataQueryExecutor,
                Executors.newFixedThreadPool(recordProcessorMaxThreads),
                getLogger());
        writeContext = new WriteContext(
                contextId,
                recordSinkManager.getRecordSinks(),
                dropExternalTableQueryExecutor,
                getLogger());
        writeContextManager.add(writeContext);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile firstFlowFile = session.get();
        if (firstFlowFile == null) {
            return;
        }

        final boolean batchingEnabled = context.getProperty(FLOW_FILE_BATCHING_ENABLED).asBoolean();
        final long batchSizeBytes = context.getProperty(FLOW_FILE_BATCH_SIZE).asDataSize(DataUnit.B).longValue();
        final List<FlowFile> batch = new ArrayList<>();
        batch.add(firstFlowFile);

        long totalBytes = firstFlowFile.getSize();
            if (batchingEnabled) {
            while (totalBytes < batchSizeBytes) {
                final FlowFile next = session.get();
            if (next == null) {
                    break;
                }
                batch.add(next);
                totalBytes += next.getSize();
                if (totalBytes >= batchSizeBytes) {
                    break;
                }
            }
        }

        final GpfdistService gpfdistService = context.getProperty(GPFDIST_SERVICE)
                .asControllerService(GpfdistService.class);
        final GreengageService greengageService = gpfdistService.getGreengageMetadataService();
        final TransferDataQueryExecutor createExternalTableQueryExecutor =
                gpfdistService.getCreateReadExternalTableQueryExecutor();

        RecordSink recordSink = null;
        boolean sinkAcquired = false;
        final StopWatch stopWatch = new StopWatch(true);

        try {
            recordSink = recordSinkManager.acquire();
            sinkAcquired = true;
            getLogger().info("Acquiring record sink: {}", recordSink);
            final String schema = context.getProperty(SCHEMA_NAME)
                    .evaluateAttributeExpressions(firstFlowFile)
                    .getValue();
            final String table = context.getProperty(TABLE_NAME)
                    .evaluateAttributeExpressions(firstFlowFile)
                    .getValue();

            final TableDescription tableDescription = greengageService.getTableDescription(schema, table);
            final String destinationUrl = greengageService.getDatabaseMetadata().getURL();
            final List<ColumnDescription> columnDescriptions = getColumnDescriptions(context, firstFlowFile, tableDescription);
            final RecordSchema baseReaderSchema;

            //get metadata from firstFlowFile, assumed that other flow files will have the same metadata
            try (final InputStream in = session.read(firstFlowFile)) {
                final RecordReader recordReader = context.getProperty(RECORD_READER_FACTORY)
                        .asControllerService(RecordReaderFactory.class)
                        .createRecordReader(firstFlowFile, in, getLogger());
                baseReaderSchema = recordReader.getSchema();
            }

            final String sinkId = recordSink.getId();
            final GpfdistLoadMetadata metadata = Optional.ofNullable(writeContext.getGpfdistMetadata(sinkId))
                    .orElseGet(() -> {
                        final GpfdistLoadMetadata created = (GpfdistLoadMetadata) gpfdistService.getGpfdistLoadMetadataFactory()
                                .create(writeContext.getContextId(), sinkId, tableDescription, columnDescriptions, baseReaderSchema);
                        return writeContext.addGpfdistLoadMetadata(sinkId, created);
                    });

            if (baseReaderSchema.getFieldCount() != metadata.getRecordSchema().getFieldCount()) {
                throw new ProcessException("Schema does not match target column count. reader="
                        + baseReaderSchema.getFieldCount() + " target=" + metadata.getRecordSchema().getFieldCount());
            }

            //create readable external table once
            writeContext.ensureExternalTableCreated(sinkId, () -> createExternalTableQueryExecutor.execute(metadata).get());

            //execute insert for the whole batch
            recordSink.beginLoad(metadata);
            for (FlowFile ff : batch) {
                try (final InputStream in = session.read(ff)) {
                    final RecordReader recordReader = context.getProperty(RECORD_READER_FACTORY)
                            .asControllerService(RecordReaderFactory.class)
                            .createRecordReader(ff, in, getLogger());

                    final RecordSchema readerSchema = recordReader.getSchema();
                    if (readerSchema.getFieldCount() != metadata.getRecordSchema().getFieldCount()) {
                        throw new ProcessException("Schema mismatch inside batch. Expected fields="
                                + metadata.getRecordSchema().getFieldCount() + " but got=" + readerSchema.getFieldCount()
                                + " for FlowFile=" + ff);
                    }

                    final RecordSet rs = recordReader.createRecordSet();
                    Record record;
                    while ((record = rs.next()) != null) {
                        recordSink.load(record);
                    }
                }
            }
            getLogger().info("Sent all records for processing for sinkId: {}", sinkId);
            final LoadResult result = recordSink.finishLoad().get();
            getLogger().info("Finished loading for sink {}", sinkId);
            if (result.isSuccess()) {
                final long elapsedMs = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
                for (FlowFile flowFile : batch) {
                session.getProvenanceReporter().send(flowFile,
                    destinationUrl,
                    "result=" + result,
                    elapsedMs
                    );
                    session.transfer(flowFile, REL_SUCCESS);
                }
                getLogger().info("Successfully loaded flow files: batchSize={}, result: {}", batch.size(), result);
            } else {
                final String errMsg = result.getErrors().stream()
                    .map(t -> t.getClass().getSimpleName() + ": " + t.getMessage())
                    .collect(Collectors.joining("; "));
                for (FlowFile ff : batch) {
                    session.penalize(ff);
                    session.transfer(ff, REL_FAILURE);
                }
                getLogger().error("Loading failed. result={} batchSize={} errors={}",
                        result,
                        batch.size(),
                        errMsg);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            if (recordSink != null) {
                try {
                    recordSink.abort();
                } catch (Exception ignored) {
                }
            }
            for (FlowFile ff : batch) {
                session.penalize(ff);
                session.transfer(ff, REL_FAILURE);
            }
            context.yield();
        } catch (Exception e) {
            if (recordSink != null) {
                try {
                    recordSink.abort();
                } catch (Exception ignored) {
                }
            }
            getLogger().error("Batch loading failed. batchSize={}", batch.size(), e);
            for (FlowFile flowFile : batch) {
                session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
        } finally {
            if (sinkAcquired && recordSink != null) {
                try {
                    recordSinkManager.release(recordSink);
                    getLogger().info("Successfully released record sink: {}", recordSink);
                } catch (Exception re) {
                    getLogger().error("Failed to release record sink back to pool {}", recordSink, re);
                    context.yield();
                }
            }
        }
    }

    @OnUnscheduled
    public void onUnschedule(final ProcessContext context) {
        try {
            if (recordSinkManager != null) {
                recordSinkManager.close();
            }
            if (writeContext != null && writeContextManager != null) {
                writeContextManager.remove(writeContext.getContextId());
                writeContext.close();
            }
        } catch (Exception e) {
            getLogger().warn("Failed to close resources for context {}", writeContext != null ? writeContext.getContextId() : null, e);
        }
    }

    private List<ColumnDescription> getColumnDescriptions(ProcessContext context,
                                                          FlowFile flowFile,
                                                          TableDescription tableDescription) {
        String rawColumns = context.getProperty(TABLE_COLUMNS)
            .evaluateAttributeExpressions(flowFile)
            .getValue();

        List<String> columns = Arrays.stream(rawColumns.split(","))
                .map(col -> col.replace(QUOTE, "").trim())
                .collect(Collectors.toList());
        List<ColumnDescription> columnDescriptions = new ArrayList<>();
        for (String s : columns) {
            ColumnDescription columnDescription = tableDescription.getColumns().get(s);
            if (columnDescription == null) {
                throw new IllegalStateException(
                    "Column " + s + " not found in table " + tableDescription.getTableName());
            }
            columnDescriptions.add(columnDescription);
        }
        return columnDescriptions;
    }

    private int getNodeIndex(GpfdistService gpfdistService) {
        try {
            return gpfdistService.getNodeIndexService().getNodeIndex();
        } catch (Exception e) {
            getLogger().error("Failed to get node index", e);
            throw new RuntimeException(e);
        }
    }
}
