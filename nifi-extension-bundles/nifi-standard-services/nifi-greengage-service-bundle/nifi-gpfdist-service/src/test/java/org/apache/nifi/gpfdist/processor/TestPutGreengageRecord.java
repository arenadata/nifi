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

import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.service.CancellableQuery;
import org.apache.nifi.gpfdist.service.GpfdistLoadMetadataFactory;
import org.apache.nifi.gpfdist.service.GreengageService;
import org.apache.nifi.gpfdist.service.TransferDataQueryExecutor;
import org.apache.nifi.gpfdist.service.datatype.IntegerDataType;
import org.apache.nifi.gpfdist.service.datatype.VarcharDataType;
import org.apache.nifi.gpfdist.service.greengage.model.GreengageColumnDescription;
import org.apache.nifi.gpfdist.service.greengage.model.GreengageTableDescription;
import org.apache.nifi.gpfdist.service.load.metadata.GpfdistLoadMetadata;
import org.apache.nifi.gpfdist.service.load.process.GpfdistRecordSink;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.sql.DatabaseMetaData;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestPutGreengageRecord {
    private static final String RECORD_READER = "MockRecordReader";
    private TestRunner testRunner;
    private MockRecordParser recordReader;
    private MockGpfdistService mockGpfdistService;
    private String tableName;
    private List<String> tableColumns;

    @BeforeEach
    void setUp() throws Exception {
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

        final GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        when(greengageService.getSegmentCount()).thenReturn(1);
        when(mockGpfdistService.getNodeIndexService().getNodeCount()).thenReturn(1);
        when(mockGpfdistService.getNodeIndexService().getNodeIndex()).thenReturn(0);
        final DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        when(databaseMetaData.getURL()).thenReturn("jdbc://test:6000");
        when(greengageService.getDatabaseMetadata()).thenReturn(databaseMetaData);

        final RecordSchema metadataRecordSchema = mock(RecordSchema.class);
        when(metadataRecordSchema.getFieldCount()).thenReturn(tableColumns.size());
        final GpfdistLoadMetadata metadata = mock(GpfdistLoadMetadata.class);
        when(metadata.getRecordSchema()).thenReturn(metadataRecordSchema);
        final GpfdistLoadMetadataFactory loadMetadataFactory = mockGpfdistService.getGpfdistLoadMetadataFactory();
        when(loadMetadataFactory.create(any(), anyString(), any(), any(), any())).thenReturn(metadata);

        final TransferDataQueryExecutor dropQueryExecutor = mockGpfdistService.getDropExternalTableQueryExecutor();
        when(dropQueryExecutor.execute(any())).thenReturn(CompletableFuture.completedFuture(null));
        final TransferDataQueryExecutor insertQueryExecutor = mockGpfdistService.getInsertDataIntoTargetTableQueryExecutor();
        when(insertQueryExecutor.executeCancellable(any())).thenAnswer(invocation -> completedCancellableQuery(CompletableFuture.completedFuture(null)));
    }

    @Test
    void testPutRecordsSuccess() throws InitializationException {
        testRunner.addControllerService(RECORD_READER, recordReader);
        testRunner.enableControllerService(recordReader);
        GreengageTableDescription tableDescription = createGreengageTableDescription(tableColumns);
        GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        TransferDataQueryExecutor queryExecutor = mockGpfdistService.getCreateReadExternalTableQueryExecutor();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(tableDescription);
        when(queryExecutor.execute(any())).thenReturn(CompletableFuture.completedFuture(null));

        testRunner.addControllerService(mockGpfdistService.getIdentifier(), mockGpfdistService);
        testRunner.enableControllerService(mockGpfdistService);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

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
        when(greengageService.getTableDescription(null, tableName)).thenReturn(tableDescription);

        testRunner.addControllerService(mockGpfdistService.getIdentifier(), mockGpfdistService);
        testRunner.enableControllerService(mockGpfdistService);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

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
        TransferDataQueryExecutor queryExecutor = mockGpfdistService.getCreateReadExternalTableQueryExecutor();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(tableDescription);
        RuntimeException queryError = new RuntimeException("Failed loading query");
        when(queryExecutor.execute(any()))
                .thenReturn(CompletableFuture.failedFuture(queryError));

        testRunner.addControllerService(mockGpfdistService.getIdentifier(), mockGpfdistService);
        testRunner.enableControllerService(mockGpfdistService);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

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
        TransferDataQueryExecutor queryExecutor = mockGpfdistService.getCreateReadExternalTableQueryExecutor();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(tableDescription);
        when(queryExecutor.execute(any())).thenReturn(CompletableFuture.completedFuture(null));
        final TransferDataQueryExecutor insertQueryExecutor = mockGpfdistService.getInsertDataIntoTargetTableQueryExecutor();
        final RuntimeException insertError = new RuntimeException("Failed loading query");
        when(insertQueryExecutor.executeCancellable(any()))
                .thenAnswer(invocation -> completedCancellableQuery(CompletableFuture.failedFuture(insertError)));

        testRunner.addControllerService(mockGpfdistService.getIdentifier(), mockGpfdistService);
        testRunner.enableControllerService(mockGpfdistService);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

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
        TransferDataQueryExecutor queryExecutor = mockGpfdistService.getCreateReadExternalTableQueryExecutor();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(tableDescription);
        when(queryExecutor.execute(any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        testRunner.addControllerService(mockGpfdistService.getIdentifier(), mockGpfdistService);
        testRunner.enableControllerService(mockGpfdistService);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);
        recordReader.addSchemaField("unknown", RecordFieldType.STRING);

        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_FAILURE, 1);
    }

    @Test
    void testPutRecordsIntoTableOfEachFlowFile() throws Exception {
        testRunner.setProperty(PutGreengageRecord.TABLE_NAME, "${table}");
        final GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        when(greengageService.getTableDescription(null, "first")).thenReturn(createGreengageTableDescription("first", 100));
        when(greengageService.getTableDescription(null, "second")).thenReturn(createGreengageTableDescription("second", 100));
        enableServicesWithMetadataPerTarget();

        testRunner.enqueue("", Map.of("table", "first"));
        testRunner.enqueue("", Map.of("table", "second"));
        testRunner.run(2);

        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_SUCCESS, 2);
        assertEquals(List.of("first", "second"), getLoadedTables());
        assertEquals(List.of("ext_1", "ext_2"), getExternalTables(mockGpfdistService.getCreateReadExternalTableQueryExecutor()));
        assertEquals(List.of("ext_1", "ext_2"), getExternalTables(mockGpfdistService.getDropExternalTableQueryExecutor()));
    }

    @Test
    void testPutRecordsBatchesOnlyFlowFilesOfSameTable() throws Exception {
        testRunner.setProperty(PutGreengageRecord.TABLE_NAME, "${table}");
        testRunner.setProperty(PutGreengageRecord.FLOW_FILE_BATCHING_ENABLED, "true");
        final GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        when(greengageService.getTableDescription(null, "first")).thenReturn(createGreengageTableDescription("first", 100));
        when(greengageService.getTableDescription(null, "second")).thenReturn(createGreengageTableDescription("second", 100));
        enableServicesWithMetadataPerTarget();

        testRunner.enqueue("", Map.of("table", "first"));
        testRunner.enqueue("", Map.of("table", "second"));
        testRunner.enqueue("", Map.of("table", "first"));
        testRunner.run(2);

        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_SUCCESS, 3);
        assertEquals(List.of("first", "second"), getLoadedTables());
    }

    @Test
    void testPutRecordsReusesExternalTableForSameTable() throws Exception {
        final GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(createGreengageTableDescription(tableName, 100));
        enableServicesWithMetadataPerTarget();

        testRunner.enqueue("");
        testRunner.enqueue("");
        testRunner.run(2);

        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_SUCCESS, 2);
        assertEquals(List.of(tableName, tableName), getLoadedTables());
        assertEquals(List.of("ext_1"), getExternalTables(mockGpfdistService.getCreateReadExternalTableQueryExecutor()));
        assertEquals(List.of("ext_1"), getExternalTables(mockGpfdistService.getDropExternalTableQueryExecutor()));
    }

    @Test
    void testPutRecordsRecreatesExternalTableWhenColumnTypeChanged() throws Exception {
        final GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(
                createGreengageTableDescription(tableName, 100),
                createGreengageTableDescription(tableName, 200));
        when(mockGpfdistService.getInsertDataIntoTargetTableQueryExecutor().executeCancellable(any()))
                .thenAnswer(invocation -> completedCancellableQuery(CompletableFuture.failedFuture(new RuntimeException("value too long"))))
                .thenAnswer(invocation -> completedCancellableQuery(CompletableFuture.completedFuture(null)));
        enableServicesWithMetadataPerTarget();

        testRunner.enqueue("");
        testRunner.enqueue("");
        testRunner.run(2);

        testRunner.assertTransferCount(PutGreengageRecord.REL_FAILURE, 1);
        testRunner.assertTransferCount(PutGreengageRecord.REL_SUCCESS, 1);
        assertEquals(List.of("ext_1", "ext_2"), getExternalTables(mockGpfdistService.getCreateReadExternalTableQueryExecutor()));
        assertEquals(List.of("ext_1", "ext_2"), getExternalTables(mockGpfdistService.getDropExternalTableQueryExecutor()));
    }

    @Test
    void testPutRecordsReadsTableDescriptionOncePerTable() throws Exception {
        final GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(createGreengageTableDescription(tableName, 100));
        enableServicesWithMetadataPerTarget();

        testRunner.enqueue("");
        testRunner.enqueue("");
        testRunner.run(2);

        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_SUCCESS, 2);
        verify(greengageService, times(1)).getTableDescription(null, tableName);
        verify(greengageService, times(1)).getDatabaseMetadata();
    }

    @Test
    void testPutRecordsBatchesAllFlowFilesOfStaticTable() throws Exception {
        testRunner.setProperty(PutGreengageRecord.FLOW_FILE_BATCHING_ENABLED, "true");
        final GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(createGreengageTableDescription(tableName, 100));
        enableServicesWithMetadataPerTarget();

        testRunner.enqueue("");
        testRunner.enqueue("");
        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_SUCCESS, 3);
        assertEquals(List.of(tableName), getLoadedTables());
    }

    @Test
    void testPutRecordsStopsBatchScanAfterRejectedFlowFileLimit() throws Exception {
        testRunner.setProperty(PutGreengageRecord.TABLE_NAME, "${table}");
        testRunner.setProperty(PutGreengageRecord.FLOW_FILE_BATCHING_ENABLED, "true");
        final GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        when(greengageService.getTableDescription(null, "first")).thenReturn(createGreengageTableDescription("first", 100));
        when(greengageService.getTableDescription(null, "second")).thenReturn(createGreengageTableDescription("second", 100));
        enableServicesWithMetadataPerTarget();

        testRunner.enqueue("", Map.of("table", "first"));
        for (int i = 0; i < 1000; i++) {
            testRunner.enqueue("", Map.of("table", "second"));
        }
        testRunner.enqueue("", Map.of("table", "first"));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_SUCCESS, 1);
        assertEquals(1001, testRunner.getQueueSize().getObjectCount());
    }

    @Test
    void testPutRecordsDropsExternalTableOnlyWhenStopped() throws Exception {
        final GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(createGreengageTableDescription(tableName, 100));
        enableServicesWithMetadataPerTarget();

        testRunner.enqueue("");
        testRunner.run(1, false);
        testRunner.unSchedule();
        assertEquals(List.of(), getExternalTables(mockGpfdistService.getDropExternalTableQueryExecutor()));

        testRunner.stop();
        assertEquals(List.of("ext_1"), getExternalTables(mockGpfdistService.getDropExternalTableQueryExecutor()));
    }

    @Test
    void testPutRecordsYieldsWhenNodeHasNoRecordSink() throws Exception {
        when(mockGpfdistService.getNodeIndexService().getNodeCount()).thenReturn(2);
        when(mockGpfdistService.getNodeIndexService().getNodeIndex()).thenReturn(1);
        enableServicesWithMetadataPerTarget();

        testRunner.enqueue("");
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> testRunner.run());

        testRunner.assertQueueNotEmpty();
        testRunner.assertTransferCount(PutGreengageRecord.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutGreengageRecord.REL_FAILURE, 0);
        assertTrue(((MockProcessContext) testRunner.getProcessContext()).isYieldCalled());
    }

    @Test
    void testPutRecordsWaitsForBusyRecordSinkWithoutYield() throws Exception {
        testRunner.setThreadCount(2);
        final GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(createGreengageTableDescription(tableName, 100));
        when(mockGpfdistService.getInsertDataIntoTargetTableQueryExecutor().executeCancellable(any()))
                .thenAnswer(invocation -> completedCancellableQuery(
                        CompletableFuture.runAsync(() -> { }, CompletableFuture.delayedExecutor(200, TimeUnit.MILLISECONDS))));
        enableServicesWithMetadataPerTarget();

        testRunner.enqueue("");
        testRunner.enqueue("");
        testRunner.run(2);

        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_SUCCESS, 2);
        assertFalse(((MockProcessContext) testRunner.getProcessContext()).isYieldCalled());
    }

    @Test
    void testPutRecordsRoutesFlowFileWithInvalidTargetToFailureWhenBatching() throws Exception {
        enableServicesWithTableFromJsonAttribute();

        testRunner.enqueue("", Map.of("table", "not json"));
        testRunner.enqueue("", Map.of("table", "{\"t\":\"first\"}"));
        testRunner.run(2);

        testRunner.assertTransferCount(PutGreengageRecord.REL_FAILURE, 1);
        testRunner.assertTransferCount(PutGreengageRecord.REL_SUCCESS, 1);
        assertEquals(List.of("first"), getLoadedTables());
    }

    @Test
    void testPutRecordsLeavesFlowFileWithInvalidTargetOutOfBatch() throws Exception {
        enableServicesWithTableFromJsonAttribute();

        testRunner.enqueue("", Map.of("table", "{\"t\":\"first\"}"));
        testRunner.enqueue("", Map.of("table", "not json"));
        testRunner.run(2);

        testRunner.assertTransferCount(PutGreengageRecord.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PutGreengageRecord.REL_FAILURE, 1);
        assertEquals(List.of("first"), getLoadedTables());
    }

    @Test
    void testPutRecordsReloadsCachedTableDescriptionWithoutRequestedColumn() throws Exception {
        testRunner.setProperty(PutGreengageRecord.TABLE_COLUMNS, "${columns}");
        final GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        final GreengageTableDescription alteredTable = new GreengageTableDescription(null, tableName, Map.of(
                "name", new GreengageColumnDescription("name", new VarcharDataType(100), true, false),
                "age", new GreengageColumnDescription("age", new IntegerDataType(), true, false),
                "sport", new GreengageColumnDescription("sport", new VarcharDataType(), false, false),
                "city", new GreengageColumnDescription("city", new VarcharDataType(), false, false)));
        when(greengageService.getTableDescription(null, tableName))
                .thenReturn(createGreengageTableDescription(tableName, 100), alteredTable);
        enableServicesWithMetadataPerTarget();

        testRunner.enqueue("", Map.of("columns", "name,age,sport"));
        testRunner.enqueue("", Map.of("columns", "name,age,city"));
        testRunner.run(2);

        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_SUCCESS, 2);
        verify(greengageService, times(2)).getTableDescription(null, tableName);
    }

    @Test
    void testPutRecordsStopsRecordProcessingThreadsWhenStopped() throws Exception {
        recordReader.addRecord("John", 30, "soccer");
        final GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        when(greengageService.getTableDescription(null, tableName)).thenReturn(createGreengageTableDescription(tableName, 100));
        final List<Thread> recordProcessingThreads = new CopyOnWriteArrayList<>();
        when(mockGpfdistService.getInsertDataIntoTargetTableQueryExecutor().executeCancellable(any()))
                .thenAnswer(invocation -> completedCancellableQuery(CompletableFuture.runAsync(() -> {
                    recordProcessingThreads.addAll(getRecordProcessingThreads());
                    throw new IllegalStateException("Insert failed");
                }, CompletableFuture.delayedExecutor(200, TimeUnit.MILLISECONDS))));
        enableServicesWithMetadataPerTarget();

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_FAILURE, 1);
        assertFalse(recordProcessingThreads.isEmpty());
        for (Thread thread : recordProcessingThreads) {
            thread.join(5000);
        }
        assertEquals(List.of(), recordProcessingThreads.stream().filter(Thread::isAlive).map(Thread::getName).toList());
    }

    @Test
    void testPutRecordsRetriesFailedExternalTableDropOnStop() throws Exception {
        testRunner.setProperty(PutGreengageRecord.TABLE_NAME, "${table}");
        final GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        when(greengageService.getTableDescription(null, "first")).thenReturn(createGreengageTableDescription("first", 100));
        when(greengageService.getTableDescription(null, "second")).thenReturn(createGreengageTableDescription("second", 100));
        when(mockGpfdistService.getDropExternalTableQueryExecutor().execute(any())).thenReturn(
                CompletableFuture.failedFuture(new RuntimeException("Drop failed")),
                CompletableFuture.completedFuture(null));
        enableServicesWithMetadataPerTarget();

        testRunner.enqueue("", Map.of("table", "first"));
        testRunner.enqueue("", Map.of("table", "second"));
        testRunner.run(2);

        testRunner.assertAllFlowFilesTransferred(PutGreengageRecord.REL_SUCCESS, 2);
        assertEquals(List.of("ext_1", "ext_2", "ext_1"), getExternalTables(mockGpfdistService.getDropExternalTableQueryExecutor()));
    }

    private static List<Thread> getRecordProcessingThreads() {
        return Thread.getAllStackTraces().entrySet().stream()
                .filter(entry -> Arrays.stream(entry.getValue())
                        .anyMatch(frame -> frame.getClassName().startsWith(GpfdistRecordSink.class.getName())))
                .map(Map.Entry::getKey)
                .toList();
    }

    private void enableServicesWithTableFromJsonAttribute() throws InitializationException {
        testRunner.setProperty(PutGreengageRecord.TABLE_NAME, "${table:jsonPath('$.t')}");
        testRunner.setProperty(PutGreengageRecord.FLOW_FILE_BATCHING_ENABLED, "true");
        final GreengageService greengageService = mockGpfdistService.getGreengageMetadataService();
        when(greengageService.getTableDescription(null, "first")).thenReturn(createGreengageTableDescription("first", 100));
        enableServicesWithMetadataPerTarget();
    }

    private void enableServicesWithMetadataPerTarget() throws InitializationException {
        testRunner.addControllerService(RECORD_READER, recordReader);
        testRunner.enableControllerService(recordReader);
        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        final AtomicInteger externalTableCount = new AtomicInteger();
        when(mockGpfdistService.getGpfdistLoadMetadataFactory().create(any(), anyString(), any(), any(), any()))
                .thenAnswer(invocation -> new GpfdistLoadMetadata("ext_" + externalTableCount.incrementAndGet(),
                        invocation.getArgument(2),
                        invocation.getArgument(3),
                        null,
                        "gpfdist://localhost:8080",
                        invocation.getArgument(4)));
        when(mockGpfdistService.getCreateReadExternalTableQueryExecutor().execute(any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        testRunner.addControllerService(mockGpfdistService.getIdentifier(), mockGpfdistService);
        testRunner.enableControllerService(mockGpfdistService);
    }

    private List<String> getLoadedTables() {
        final ArgumentCaptor<GpfdistMetadata> captor = ArgumentCaptor.forClass(GpfdistMetadata.class);
        verify(mockGpfdistService.getInsertDataIntoTargetTableQueryExecutor(), atLeast(0)).executeCancellable(captor.capture());
        return captor.getAllValues().stream()
                .map(metadata -> metadata.getTableMetadata().getTableName())
                .toList();
    }

    private List<String> getExternalTables(final TransferDataQueryExecutor queryExecutor) {
        final ArgumentCaptor<GpfdistMetadata> captor = ArgumentCaptor.forClass(GpfdistMetadata.class);
        verify(queryExecutor, atLeast(0)).execute(captor.capture());
        return captor.getAllValues().stream()
                .map(GpfdistMetadata::getExternalTable)
                .toList();
    }

    private CancellableQuery completedCancellableQuery(final CompletableFuture<Void> queryFuture) {
        return new CancellableQuery() {
            @Override
            public CompletableFuture<Void> future() {
                return queryFuture;
            }

            @Override
            public void cancel() {
                queryFuture.cancel(true);
            }
        };
    }

    private GreengageTableDescription createGreengageTableDescription(List<String> columnNames) {
        return new GreengageTableDescription(null, tableName, Map.of(
                columnNames.get(0), new GreengageColumnDescription(columnNames.get(0), new VarcharDataType(100), true, false),
                columnNames.get(1), new GreengageColumnDescription(columnNames.get(1), new IntegerDataType(), true, false),
                columnNames.get(2), new GreengageColumnDescription(columnNames.get(2), new VarcharDataType(), false, false)));
    }

    private GreengageTableDescription createGreengageTableDescription(final String table, final int nameLength) {
        return new GreengageTableDescription(null, table, Map.of(
                "name", new GreengageColumnDescription("name", new VarcharDataType(nameLength), true, false),
                "age", new GreengageColumnDescription("age", new IntegerDataType(), true, false),
                "sport", new GreengageColumnDescription("sport", new VarcharDataType(), false, false)));
    }
}
