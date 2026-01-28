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

import org.apache.nifi.gpfdist.metadata.ContextId;
import org.apache.nifi.gpfdist.service.GreengageService;
import org.apache.nifi.gpfdist.service.RecordSink;
import org.apache.nifi.gpfdist.service.RecordSinkProvider;
import org.apache.nifi.gpfdist.service.TransferDataQueryExecutor;
import org.apache.nifi.gpfdist.service.datatype.IntegerDataType;
import org.apache.nifi.gpfdist.service.datatype.VarcharDataType;
import org.apache.nifi.gpfdist.service.greengage.model.GreengageColumnDescription;
import org.apache.nifi.gpfdist.service.greengage.model.GreengageTableDescription;
import org.apache.nifi.gpfdist.service.load.context.WriteContext;
import org.apache.nifi.gpfdist.service.load.metadata.GpfdistLoadMetadata;
import org.apache.nifi.gpfdist.service.load.metadata.LoadingResult;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestPutGreengageRecord {
    private static final String RECORD_READER = "MockRecordReader";
    private TestRunner testRunner;
    private MockRecordParser recordReader;
    private MockGpfdistService mockGpfdistService;
    private String tableName;
    private List<String> tableColumns;

    @BeforeEach
    void setUp() {
        PutGreengageRecord processor = new PutGreengageRecord();
        testRunner = TestRunners.newTestRunner(processor);
        recordReader = new MockRecordParser();
        mockGpfdistService = new MockGpfdistService();
        testRunner.setProperty(PutGreengageRecord.RECORD_READER_FACTORY, RECORD_READER);
        tableName = "test";
        tableColumns = List.of("name", "age", "sport");
        testRunner.setProperty(PutGreengageRecord.TABLE_NAME, tableName);
        testRunner.setProperty(PutGreengageRecord.TABLE_COLUMNS, String.join(",", tableColumns));
        testRunner.setProperty(PutGreengageRecord.GPFDIST_SERVICE, mockGpfdistService.getIdentifier());
    }

    @Test
    void testPutRecordsSuccess() throws InitializationException {
        testRunner.addControllerService(RECORD_READER, recordReader);
        testRunner.enableControllerService(recordReader);
        GreengageTableDescription tableDescription = createGreengageTableDescription(tableColumns);
        GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        RecordSinkProvider recordSinkProvider = mockGpfdistService.getRecordSinkProvider();
        TransferDataQueryExecutor queryExecutor = mockGpfdistService.getLoadDataQueryExecutor();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(tableDescription);
        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        try {
            when(databaseMetaData.getURL()).thenReturn("jdbc://test:6000");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        when(greengageService.getDatabaseMetadata()).thenReturn(databaseMetaData);
        RecordSink recordSink = mock(RecordSink.class);
        WriteContext writeContext = mock(WriteContext.class);
        GpfdistLoadMetadata metadata = mock(GpfdistLoadMetadata.class);
        when(writeContext.getMetadata()).thenReturn(metadata);
        when(writeContext.getResult()).thenReturn(new LoadingResult());
        when(recordSink.getContext()).thenReturn(writeContext);
        when(recordSinkProvider.createRecordSink(any(ContextId.class), eq(tableDescription), any(List.class), any(RecordSchema.class)))
                .thenReturn(recordSink);
        when(queryExecutor.execute(eq(metadata))).thenReturn(CompletableFuture.completedFuture(null));
        doNothing().when(recordSink).load(any());
        when(recordSink.finish()).thenReturn(CompletableFuture.completedFuture(null));

        testRunner.addControllerService(mockGpfdistService.getIdentifier(), mockGpfdistService);
        testRunner.enableControllerService(mockGpfdistService);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, "Soccer");
        recordReader.addRecord("Jane Doe", 47, "Tennis");
        recordReader.addRecord("Sally Doe", 47, "Curling");
        recordReader.addRecord("Jimmy Doe", 14, null);
        recordReader.addRecord("Pizza Doe", 14, null);

        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_SUCCESS, 1);
    }

    @Test
    void testPutRecordsWithNonExistsTableColumnFailed() throws InitializationException {
        testRunner.addControllerService(RECORD_READER, recordReader);
        testRunner.enableControllerService(recordReader);
        GreengageTableDescription tableDescription = createGreengageTableDescription(List.of(tableColumns.get(0),
                tableColumns.get(1),
                "nonExistColumn"));
        GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        RecordSinkProvider recordSinkProvider = mockGpfdistService.getRecordSinkProvider();
        TransferDataQueryExecutor queryExecutor = mockGpfdistService.getLoadDataQueryExecutor();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(tableDescription);
        RecordSink recordSink = mock(RecordSink.class);
        WriteContext writeContext = mock(WriteContext.class);
        GpfdistLoadMetadata metadata = mock(GpfdistLoadMetadata.class);
        when(writeContext.getMetadata()).thenReturn(metadata);
        when(recordSink.getContext()).thenReturn(writeContext);
        when(recordSinkProvider.createRecordSink(any(ContextId.class), eq(tableDescription), any(List.class), any(RecordSchema.class)))
                .thenReturn(recordSink);
        when(queryExecutor.execute(eq(metadata))).thenReturn(CompletableFuture.completedFuture(null));
        doNothing().when(recordSink).load(any());
        when(recordSink.finish()).thenReturn(CompletableFuture.completedFuture(null));

        testRunner.addControllerService(mockGpfdistService.getIdentifier(), mockGpfdistService);
        testRunner.enableControllerService(mockGpfdistService);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, "Soccer");
        recordReader.addRecord("Jane Doe", 47, "Tennis");
        recordReader.addRecord("Sally Doe", 47, "Curling");
        recordReader.addRecord("Jimmy Doe", 14, null);
        recordReader.addRecord("Pizza Doe", 14, null);

        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_FAILURE, 1);
    }

    @Test
    void testPutRecordsQueryLoadingFailed() throws InitializationException {
        testRunner.addControllerService(RECORD_READER, recordReader);
        testRunner.enableControllerService(recordReader);
        GreengageTableDescription tableDescription = createGreengageTableDescription(tableColumns);
        GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        RecordSinkProvider recordSinkProvider = mockGpfdistService.getRecordSinkProvider();
        TransferDataQueryExecutor queryExecutor = mockGpfdistService.getLoadDataQueryExecutor();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(tableDescription);
        RecordSink recordSink = mock(RecordSink.class);
        WriteContext writeContext = mock(WriteContext.class);
        GpfdistLoadMetadata metadata = mock(GpfdistLoadMetadata.class);
        when(writeContext.getMetadata()).thenReturn(metadata);
        when(recordSink.getContext()).thenReturn(writeContext);
        when(recordSinkProvider.createRecordSink(any(ContextId.class), eq(tableDescription), any(List.class), any(RecordSchema.class)))
                .thenReturn(recordSink);
        RuntimeException queryError = new RuntimeException("Failed loading query");
        when(queryExecutor.execute(eq(metadata)))
                .thenReturn(CompletableFuture.failedFuture(queryError));
        doNothing().when(recordSink).load(any());
        when(recordSink.finish()).thenReturn(CompletableFuture.completedFuture(null));

        testRunner.addControllerService(mockGpfdistService.getIdentifier(), mockGpfdistService);
        testRunner.enableControllerService(mockGpfdistService);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, "Soccer");
        recordReader.addRecord("Jane Doe", 47, "Tennis");
        recordReader.addRecord("Sally Doe", 47, "Curling");
        recordReader.addRecord("Jimmy Doe", 14, null);
        recordReader.addRecord("Pizza Doe", 14, null);

        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_FAILURE, 1);
    }

    @Test
    void testPutRecordsWhenRecordSinkFinishFailed() throws InitializationException {
        testRunner.addControllerService(RECORD_READER, recordReader);
        testRunner.enableControllerService(recordReader);
        GreengageTableDescription tableDescription = createGreengageTableDescription(tableColumns);
        GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        RecordSinkProvider recordSinkProvider = mockGpfdistService.getRecordSinkProvider();
        TransferDataQueryExecutor queryExecutor = mockGpfdistService.getLoadDataQueryExecutor();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(tableDescription);
        RecordSink recordSink = mock(RecordSink.class);
        WriteContext writeContext = mock(WriteContext.class);
        GpfdistLoadMetadata metadata = mock(GpfdistLoadMetadata.class);
        when(writeContext.getMetadata()).thenReturn(metadata);
        when(recordSink.getContext()).thenReturn(writeContext);
        when(recordSinkProvider.createRecordSink(any(ContextId.class), eq(tableDescription), any(List.class), any(RecordSchema.class)))
                .thenReturn(recordSink);
        RuntimeException recordSinkError = new RuntimeException("Failed loading query");
        when(queryExecutor.execute(eq(metadata)))
                .thenReturn(CompletableFuture.completedFuture(null));
        doNothing().when(recordSink).load(any());
        when(recordSink.finish()).thenReturn(CompletableFuture.failedFuture(recordSinkError));

        testRunner.addControllerService(mockGpfdistService.getIdentifier(), mockGpfdistService);
        testRunner.enableControllerService(mockGpfdistService);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, "Soccer");
        recordReader.addRecord("Jane Doe", 47, "Tennis");
        recordReader.addRecord("Sally Doe", 47, "Curling");
        recordReader.addRecord("Jimmy Doe", 14, null);
        recordReader.addRecord("Pizza Doe", 14, null);

        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_FAILURE, 1);
    }

    @Test
    void testPutRecordsWhenRecordSchemaDoesNotMatchedTargetTableColumns() throws InitializationException {
        testRunner.addControllerService(RECORD_READER, recordReader);
        testRunner.enableControllerService(recordReader);
        GreengageTableDescription tableDescription = createGreengageTableDescription(tableColumns);
        GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        RecordSinkProvider recordSinkProvider = mockGpfdistService.getRecordSinkProvider();
        TransferDataQueryExecutor queryExecutor = mockGpfdistService.getLoadDataQueryExecutor();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(tableDescription);
        RecordSink recordSink = mock(RecordSink.class);
        WriteContext writeContext = mock(WriteContext.class);
        GpfdistLoadMetadata metadata = mock(GpfdistLoadMetadata.class);
        when(writeContext.getMetadata()).thenReturn(metadata);
        when(recordSink.getContext()).thenReturn(writeContext);
        when(recordSinkProvider.createRecordSink(any(ContextId.class), eq(tableDescription), any(List.class), any(RecordSchema.class)))
                .thenReturn(recordSink);
        when(queryExecutor.execute(eq(metadata)))
                .thenReturn(CompletableFuture.completedFuture(null));
        doNothing().when(recordSink).load(any());
        when(recordSink.finish()).thenReturn(CompletableFuture.completedFuture(null));

        testRunner.addControllerService(mockGpfdistService.getIdentifier(), mockGpfdistService);
        testRunner.enableControllerService(mockGpfdistService);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);
        recordReader.addSchemaField("unknown", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, "Soccer", null);
        recordReader.addRecord("Jane Doe", 47, "Tennis", null);
        recordReader.addRecord("Sally Doe", 47, "Curling", null);
        recordReader.addRecord("Jimmy Doe", 14, null, null);
        recordReader.addRecord("Pizza Doe", 14, null, null);

        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_FAILURE, 1);
    }

    private GreengageTableDescription createGreengageTableDescription(List<String> columnNames) {
        return new GreengageTableDescription(null, tableName, Map.of(
                columnNames.get(0), new GreengageColumnDescription(columnNames.get(0), new VarcharDataType(100), true, false),
                columnNames.get(1), new GreengageColumnDescription(columnNames.get(1), new IntegerDataType(), true, false),
                columnNames.get(2), new GreengageColumnDescription(columnNames.get(2), new VarcharDataType(), false, false)));
    }
}