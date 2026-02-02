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

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.TableDescription;
import org.apache.nifi.gpfdist.service.GpfdistService;
import org.apache.nifi.gpfdist.service.GreengageService;
import org.apache.nifi.gpfdist.service.RecordSink;
import org.apache.nifi.gpfdist.service.TransferDataQueryExecutor;
import org.apache.nifi.gpfdist.service.load.context.WriteContext;
import org.apache.nifi.processor.AbstractProcessor;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.QUOTE;

@EventDriven
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
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
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
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from input records.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if records cannot be loaded into Greengage.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS;

    static {
        RELATIONSHIPS = new HashSet<>();
        RELATIONSHIPS.add(REL_SUCCESS);
        RELATIONSHIPS.add(REL_FAILURE);
    }

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;

    static {
        PROPERTY_DESCRIPTORS = new ArrayList<>();
        PROPERTY_DESCRIPTORS.add(RECORD_READER_FACTORY);
        PROPERTY_DESCRIPTORS.add(GPFDIST_SERVICE);
        PROPERTY_DESCRIPTORS.add(SCHEMA_NAME);
        PROPERTY_DESCRIPTORS.add(TABLE_NAME);
        PROPERTY_DESCRIPTORS.add(TABLE_COLUMNS);
    }

    private final Set<RecordSink> recordSinks = ConcurrentHashMap.newKeySet();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnStopped
    public void onStopped() {
        for (RecordSink recordSink : recordSinks) {
            try {
                recordSink.abort();
            } catch (Exception e) {
                getLogger().error("Error while trying to abort record sink of {}", recordSink, e);
            }
        }

        recordSinks.clear();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final String schema = context.getProperty(SCHEMA_NAME)
                .evaluateAttributeExpressions(flowFile)
                .getValue();
        final String table = context.getProperty(TABLE_NAME)
                .evaluateAttributeExpressions(flowFile)
                .getValue();
        final GpfdistService gpfdistService = context.getProperty(GPFDIST_SERVICE)
            .asControllerService(GpfdistService.class);
        final TransferDataQueryExecutor transferDataQueryExecutor = gpfdistService.getQueryExecutor();
        final GreengageService greengageService = gpfdistService.getGreengageTableService();
        final TableDescription tableDescription = greengageService.getTableDescription(schema, table);
        final StopWatch stopWatch = new StopWatch(true);

        RecordSink recordSink = null;
        try (final InputStream in = session.read(flowFile)) {
            final String destinationUrl = greengageService.getDatabaseMetadata().getURL();
            final List<ColumnDescription> columnDescriptions = getColumnDescriptions(flowFile, context, tableDescription);
            final RecordReader recordReader = context.getProperty(RECORD_READER_FACTORY)
                .asControllerService(RecordReaderFactory.class)
                .createRecordReader(flowFile, in, getLogger());

            RecordSchema readerSchema = recordReader.getSchema();
            if (readerSchema.getFieldCount() != columnDescriptions.size()) {
                throw new ProcessException("Schema does not match target column count");
            }
            recordSink = gpfdistService.getRecordSinkProvider()
                .createRecordSink(tableDescription, columnDescriptions, readerSchema);
            recordSinks.add(recordSink);
            WriteContext writeContext = (WriteContext) recordSink.getContext();

            CompletableFuture<Void> queryLoadFuture = transferDataQueryExecutor.execute(writeContext.getMetadata());
            final RecordSet recordSet = recordReader.createRecordSet();
            Record record;
            while ((record = recordSet.next()) != null && !queryLoadFuture.isCompletedExceptionally()) {
                recordSink.load(record);
            }

            final List<Throwable> processingErrors = allOfWithExceptions(recordSink.finish(), queryLoadFuture).get();
            if (processingErrors.isEmpty()) {
                session.getProvenanceReporter().send(flowFile,
                        destinationUrl,
                        writeContext.getResult().toString(),
                        stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            } else {
                throw new RuntimeException(processingErrors.stream()
                        .map(Throwable::getMessage)
                        .collect(Collectors.joining(";")));
            }
        } catch (Exception e) {
            Optional.ofNullable(recordSink).ifPresent(RecordSink::abort);
            getLogger().error("Sending record failed {}", flowFile, e);
            session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        } finally {
            Optional.ofNullable(recordSink).ifPresent(recordSinks::remove);
        }
        session.transfer(flowFile, REL_SUCCESS);
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<List<Throwable>> allOfWithExceptions(CompletableFuture<?>... futures) {
        CompletableFuture<Throwable>[] futuresWithErrors = Arrays.stream(futures)
            .map(this::withErrorRecording)
            .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(futuresWithErrors)
            .thenApply(ignored -> Arrays.stream(futuresWithErrors)
                .map(CompletableFuture::join)
                .filter(Objects::nonNull)
                .collect(Collectors.toList())
            );
    }

    private CompletableFuture<Throwable> withErrorRecording(CompletableFuture<?> future) {
        return future.handle((ignored, ex) -> {
            if (ex instanceof CompletionException && ex.getCause() != null) {
                return ex.getCause();
            }
            return ex;
        });
    }

    private List<ColumnDescription> getColumnDescriptions(FlowFile flowFile,
                                                          ProcessContext context,
                                                          TableDescription tableDescription) {
        List<String> columns = Arrays.stream(context.getProperty(TABLE_COLUMNS)
                        .evaluateAttributeExpressions(flowFile)
                        .getValue().split(","))
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
