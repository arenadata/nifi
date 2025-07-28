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
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.TableDescription;
import org.apache.nifi.gpfdist.service.*;
import org.apache.nifi.gpfdist.service.load.context.WriteContext;
import org.apache.nifi.logging.ComponentLog;
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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.gpfdist.service.util.GreenplumUtil.QUOTE;

@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"record", "put", "greenplum"})
@CapabilityDescription("Writes the contents of a FlowFile to Greenplum")
public class PutGreenplumRecord extends AbstractProcessor {
    static final PropertyDescriptor GPFDIST_SERVICE = new PropertyDescriptor.Builder()
            .name("gpfdist-record-processing-service")
            .displayName("Gpfdist Service")
            .description("The Controller Service that is used to load records into greenplum.")
            .required(true)
            .identifiesControllerService(GpfdistService.class)
            .build();
    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("put-greenplum-record-record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("put-greenplum-record-schema-name")
            .displayName("Schema Name")
            .description("The name of the schema where the data will be loaded.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("put-greenplum-record-table-name")
            .displayName("Table Name")
            .description("Name of the table where the data will be loaded.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor TABLE_COLUMNS = new PropertyDescriptor.Builder()
            .name("put-greenplum-table-columns")
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
            .description("A FlowFile is routed to this relationship if records cannot be loaded into Greenplum.")
            .build();
    protected static Set<Relationship> relationships;
    protected static List<PropertyDescriptor> propDescriptors;
    private CompletableFuture<Void> queryLoadFuture;
    private RecordSink recordSink;

    static {
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(RECORD_READER_FACTORY);
        pds.add(GPFDIST_SERVICE);
        pds.add(SCHEMA_NAME);
        pds.add(TABLE_NAME);
        pds.add(TABLE_COLUMNS);
        propDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        if (recordSink != null) {
            recordSink.abort();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        ComponentLog logger = getLogger();
        final String schema = context.getProperty(SCHEMA_NAME).getValue();
        final String table = context.getProperty(TABLE_NAME).getValue();
        final GpfdistService gpfdistService = context.getProperty(GPFDIST_SERVICE).asControllerService(GpfdistService.class);
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);

        final TransferDataQueryExecutor transferDataQueryExecutor = gpfdistService.getQueryExecutor();
        final RecordSinkProvider recordSinkProvider = gpfdistService.getRecordSinkProvider();
        final GreenplumService greenplumService = gpfdistService.getGreenplumTableService();
        final TableDescription tableDescription = greenplumService.getTableDescription(schema, table);
        final StopWatch stopWatch = new StopWatch(true);
        try (final InputStream in = session.read(flowFile)) {
            final String destinationUrl = greenplumService.getDatabaseMetadata().getURL();
            final List<ColumnDescription> columnDescriptions = getColumnDescriptions(context, tableDescription);
            final List<Throwable> errors = new ArrayList<>();
            final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, logger);
            RecordSchema readerSchema = recordReader.getSchema();
            if (readerSchema.getFieldCount() != columnDescriptions.size()) {
                throw new ProcessException("Schema does not match target column count");
            }
            recordSink = recordSinkProvider.createRecordSink(tableDescription, columnDescriptions, readerSchema);
            WriteContext writeContext = (WriteContext) recordSink.getContext();
            queryLoadFuture = transferDataQueryExecutor.execute(writeContext.getMetadata())
                    .exceptionally(ex -> {
                        errors.add(ex);
                        return null;
                    });
            final RecordSet recordSet = recordReader.createRecordSet();
            Record record;
            while ((record = recordSet.next()) != null && errors.isEmpty()) {
                recordSink.load(record);
            }
            finishLoading(errors);
            if (errors.isEmpty()) {
                session.getProvenanceReporter().send(flowFile,
                        destinationUrl,
                        writeContext.getResult().toString(),
                        stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            } else {
                recordSink.abort();
                throw new RuntimeException(errors.stream()
                        .map(Throwable::getMessage)
                        .collect(Collectors.joining(";")));
            }
        } catch (Exception e) {
            logger.error("Sending record failed {}", flowFile, e);
            session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        session.transfer(flowFile, REL_SUCCESS);
    }

    private List<ColumnDescription> getColumnDescriptions(ProcessContext context,
                                                          TableDescription tableDescription) {
        List<String> columns = Arrays.stream(context.getProperty(TABLE_COLUMNS).getValue().split(","))
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

    private void finishLoading(List<Throwable> errors) throws InterruptedException, ExecutionException {
        CompletableFuture<Void> finishFuture = recordSink.finish()
                .exceptionally(ex -> {
                    errors.add(ex);
                    return null;
                });
        List<CompletableFuture<Void>> futures = Arrays.asList(finishFuture, queryLoadFuture);
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(future -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()))
                .get();
    }
}
