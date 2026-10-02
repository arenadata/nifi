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
package org.apache.nifi.services.iceberg.hive;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processors.iceberg.PutIcebergRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.services.iceberg.parquet.ParquetIcebergWriter;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.EnabledIfDockerAvailable;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iceberg.FileFormat.PARQUET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the Controller Service against a Hive Metastore in a container, covering the paths that cannot be reached
 * without a Metastore: namespace and table operations over Thrift, Data File reads and writes through Hadoop File IO,
 * and successful verification. Skipped when Docker is not available. The image can be replaced with one matching the
 * cluster under test using -Dhive.metastore.image=<image>.
 */
@Testcontainers
@EnabledIfDockerAvailable
class HiveMetastoreIcebergCatalogMetastoreTest {
    private static final String IMAGE_PROPERTY = "hive.metastore.image";

    private static final String IMAGE_DEFAULT = "apache/hive:4.0.1";

    private static final int METASTORE_PORT = 9083;

    private static final String METASTORE_URI_FORMAT = "thrift://%s:%d";

    private static final String SERVICE_ID = "hive-metastore-catalog";

    private static final String SECOND_SERVICE_ID = "hive-metastore-catalog-second";

    private static final String PROCESSOR_SERVICE_ID = "hive-metastore-catalog-processor";

    private static final String WRITER_SERVICE_ID = "parquet-iceberg-writer";

    private static final String READER_SERVICE_ID = "record-reader";

    private static final String CATALOG_PROPERTY = "Iceberg Catalog";

    private static final String WRITER_PROPERTY = "Iceberg Writer";

    private static final String READER_PROPERTY = "Record Reader";

    private static final String NAMESPACE_PROPERTY = "Namespace";

    private static final String TABLE_NAME_PROPERTY = "Table Name";

    private static final String SUCCESS_RELATIONSHIP = "success";

    private static final String NAMESPACE_FORMAT = "nifi_ads_3777_%d";

    private static final String TABLE_NAME = "records";

    private static final String MISSING_TABLE_NAME = "records_missing";

    private static final String TABLE_DEFAULT_PROPERTY = "table-default.nifi.catalog.test";

    private static final String TABLE_PROPERTY = "nifi.catalog.test";

    private static final String TABLE_PROPERTY_VALUE = "ADS-3777";

    private static final String CONFIGURATION_STEP = "Catalog Configuration";

    private static final String CONNECTION_STEP = "Metastore Connection";

    private static final String IDENTIFIER_FIELD = "id";

    private static final String DESCRIPTION_FIELD = "description";

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, IDENTIFIER_FIELD, Types.LongType.get()),
            Types.NestedField.optional(2, DESCRIPTION_FIELD, Types.StringType.get())
    );

    private static final AtomicInteger NAMESPACE_COUNTER = new AtomicInteger();

    private static final Path WAREHOUSE = getWarehouseDirectory();

    @Container
    private static final GenericContainer<?> METASTORE = new GenericContainer<>(DockerImageName.parse(System.getProperty(IMAGE_PROPERTY, IMAGE_DEFAULT)))
            .withEnv("SERVICE_NAME", "metastore")
            .withExposedPorts(METASTORE_PORT)
            .withFileSystemBind(WAREHOUSE.toString(), WAREHOUSE.toString(), BindMode.READ_WRITE)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)));

    private TestRunner runner;

    private HiveMetastoreIcebergCatalog catalogService;

    private Namespace namespace;

    private TableIdentifier tableIdentifier;

    @BeforeEach
    void setCatalogService() throws InitializationException {
        namespace = Namespace.of(NAMESPACE_FORMAT.formatted(NAMESPACE_COUNTER.incrementAndGet()));
        tableIdentifier = TableIdentifier.of(namespace, TABLE_NAME);

        catalogService = new HiveMetastoreIcebergCatalog();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService(SERVICE_ID, catalogService);
        setCatalogProperties(catalogService);
    }

    @AfterEach
    void disableCatalogService() {
        if (runner.isControllerServiceEnabled(catalogService)) {
            final HiveCatalog catalog = catalogService.getHiveCatalog();
            if (catalog.tableExists(tableIdentifier)) {
                catalog.dropTable(tableIdentifier);
            }
            if (catalog.namespaceExists(namespace)) {
                catalog.dropNamespace(namespace);
            }
            runner.disableControllerService(catalogService);
        }
    }

    @Test
    void testVerifySuccessfulAgainstMetastore() {
        final Map<PropertyDescriptor, String> properties = Map.of(
                HiveMetastoreIcebergCatalog.METASTORE_URI, getMetastoreUri(),
                HiveMetastoreIcebergCatalog.WAREHOUSE_LOCATION, WAREHOUSE.toUri().toString()
        );
        final ConfigurationContext context = new MockConfigurationContext(catalogService, properties, null, null);

        final List<ConfigVerificationResult> results = catalogService.verify(context, runner.getLogger(), Map.of());

        assertEquals(2, results.size());
        assertEquals(CONFIGURATION_STEP, results.get(0).getVerificationStepName());
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, results.get(0).getOutcome());
        assertEquals(CONNECTION_STEP, results.get(1).getVerificationStepName());
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, results.get(1).getOutcome(), results.get(1).getExplanation());
    }

    @Test
    void testCreateNamespaceAndTable() {
        runner.enableControllerService(catalogService);
        final HiveCatalog catalog = catalogService.getHiveCatalog();

        catalog.createNamespace(namespace);
        assertTrue(catalog.namespaceExists(namespace));
        assertTrue(catalog.listNamespaces(Namespace.empty()).contains(namespace));

        final Table created = catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());
        assertNotNull(created);
        assertTrue(catalog.tableExists(tableIdentifier));

        final Table loaded = catalog.loadTable(tableIdentifier);
        assertEquals(SCHEMA.asStruct(), loaded.schema().asStruct());
        assertTrue(loaded.location().startsWith(WAREHOUSE.toUri().toString()) || loaded.location().contains(WAREHOUSE.toString()), loaded.location());
    }

    @Test
    void testLoadTableNotFound() {
        runner.enableControllerService(catalogService);
        final HiveCatalog catalog = catalogService.getHiveCatalog();
        catalog.createNamespace(namespace);

        final TableIdentifier missing = TableIdentifier.of(namespace, MISSING_TABLE_NAME);

        assertFalse(catalog.tableExists(missing));
        assertThrows(NoSuchTableException.class, () -> catalog.loadTable(missing));
    }

    @Test
    void testTableIoWritesAndReadsDataFiles() throws IOException {
        runner.enableControllerService(catalogService);
        final HiveCatalog catalog = catalogService.getHiveCatalog();
        catalog.createNamespace(namespace);
        final Table table = catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());

        assertInstanceOf(HadoopFileIO.class, table.io());

        final DataFile dataFile = writeDataFile(table, 2);
        table.newAppend().appendFile(dataFile).commit();

        final List<Record> records = readRecords(catalog.loadTable(tableIdentifier));
        assertEquals(2, records.size());
        assertEquals(0L, records.get(0).getField(IDENTIFIER_FIELD));
        assertEquals(1L, records.get(1).getField(IDENTIFIER_FIELD));
    }

    @Test
    void testCommitVisibleToSecondServiceInstance() throws IOException, InitializationException {
        runner.enableControllerService(catalogService);
        final HiveCatalog catalog = catalogService.getHiveCatalog();
        catalog.createNamespace(namespace);
        final Table table = catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(writeDataFile(table, 1)).commit();

        final HiveMetastoreIcebergCatalog secondService = new HiveMetastoreIcebergCatalog();
        runner.addControllerService(SECOND_SERVICE_ID, secondService);
        setCatalogProperties(secondService);
        runner.enableControllerService(secondService);

        try {
            final HiveCatalog secondCatalog = secondService.getHiveCatalog();
            final Table secondTable = secondCatalog.loadTable(tableIdentifier);

            assertNotNull(secondTable.currentSnapshot());
            assertEquals(1, readRecords(secondTable).size());
        } finally {
            runner.disableControllerService(secondService);
        }
    }

    @Test
    void testDynamicPropertyAppliedToTable() {
        runner.setProperty(catalogService, TABLE_DEFAULT_PROPERTY, TABLE_PROPERTY_VALUE);
        runner.enableControllerService(catalogService);
        final HiveCatalog catalog = catalogService.getHiveCatalog();
        catalog.createNamespace(namespace);

        final Table table = catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());

        assertEquals(TABLE_PROPERTY_VALUE, table.properties().get(TABLE_PROPERTY));
    }

    @Test
    void testMetastoreUriFromConfigurationResources() throws IOException {
        final Path configuration = Files.createTempFile(WAREHOUSE, "hive-site", ".xml");
        final String properties = """
                <?xml version="1.0" encoding="UTF-8"?>
                <configuration>
                    <property><name>hive.metastore.uris</name><value>%s</value></property>
                    <property><name>hive.metastore.warehouse.dir</name><value>%s</value></property>
                </configuration>""".formatted(getMetastoreUri(), WAREHOUSE.toUri());
        Files.writeString(configuration, properties);

        runner.removeProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI);
        runner.removeProperty(catalogService, HiveMetastoreIcebergCatalog.WAREHOUSE_LOCATION);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, configuration.toString());
        runner.enableControllerService(catalogService);

        final HiveCatalog catalog = catalogService.getHiveCatalog();
        catalog.createNamespace(namespace);

        assertTrue(catalog.namespaceExists(namespace));
    }

    @Test
    void testPutIcebergRecordAppendsRecordsToTable() throws IOException, InitializationException {
        runner.enableControllerService(catalogService);
        final HiveCatalog catalog = catalogService.getHiveCatalog();
        catalog.createNamespace(namespace);
        catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());

        final TestRunner processorRunner = TestRunners.newTestRunner(PutIcebergRecord.class);
        final HiveMetastoreIcebergCatalog processorCatalog = new HiveMetastoreIcebergCatalog();
        processorRunner.addControllerService(PROCESSOR_SERVICE_ID, processorCatalog);
        processorRunner.setProperty(processorCatalog, HiveMetastoreIcebergCatalog.METASTORE_URI, getMetastoreUri());
        processorRunner.setProperty(processorCatalog, HiveMetastoreIcebergCatalog.WAREHOUSE_LOCATION, WAREHOUSE.toUri().toString());
        processorRunner.enableControllerService(processorCatalog);

        final ParquetIcebergWriter icebergWriter = new ParquetIcebergWriter();
        processorRunner.addControllerService(WRITER_SERVICE_ID, icebergWriter);
        processorRunner.enableControllerService(icebergWriter);

        final MockRecordParser recordReader = new MockRecordParser();
        recordReader.addSchemaField(IDENTIFIER_FIELD, RecordFieldType.LONG);
        recordReader.addSchemaField(DESCRIPTION_FIELD, RecordFieldType.STRING);
        recordReader.addRecord(0L, "record-0");
        recordReader.addRecord(1L, "record-1");
        processorRunner.addControllerService(READER_SERVICE_ID, recordReader);
        processorRunner.enableControllerService(recordReader);

        processorRunner.setProperty(CATALOG_PROPERTY, PROCESSOR_SERVICE_ID);
        processorRunner.setProperty(WRITER_PROPERTY, WRITER_SERVICE_ID);
        processorRunner.setProperty(READER_PROPERTY, READER_SERVICE_ID);
        processorRunner.setProperty(NAMESPACE_PROPERTY, namespace.level(0));
        processorRunner.setProperty(TABLE_NAME_PROPERTY, TABLE_NAME);

        processorRunner.enqueue(new byte[0]);

        try {
            processorRunner.run();

            processorRunner.assertAllFlowFilesTransferred(SUCCESS_RELATIONSHIP, 1);

            final Table appended = catalog.loadTable(tableIdentifier);
            assertNotNull(appended.currentSnapshot());

            final List<Record> records = readRecords(appended);
            assertEquals(2, records.size());
            assertEquals(0L, records.get(0).getField(IDENTIFIER_FIELD));
            assertEquals("record-1", records.get(1).getField(DESCRIPTION_FIELD));
        } finally {
            processorRunner.disableControllerService(processorCatalog);
        }
    }

    private void setCatalogProperties(final HiveMetastoreIcebergCatalog service) {
        runner.setProperty(service, HiveMetastoreIcebergCatalog.METASTORE_URI, getMetastoreUri());
        runner.setProperty(service, HiveMetastoreIcebergCatalog.WAREHOUSE_LOCATION, WAREHOUSE.toUri().toString());
    }

    private String getMetastoreUri() {
        return METASTORE_URI_FORMAT.formatted(METASTORE.getHost(), METASTORE.getMappedPort(METASTORE_PORT));
    }

    private DataFile writeDataFile(final Table table, final int count) throws IOException {
        final OutputFile outputFile = table.io().newOutputFile("%s/data/%s.parquet".formatted(table.location(), System.nanoTime()));
        final GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema());

        try (DataWriter<Record> writer = appenderFactory.newDataWriter(EncryptedFiles.plainAsEncryptedOutput(outputFile), PARQUET, null)) {
            for (int i = 0; i < count; i++) {
                final GenericRecord record = GenericRecord.create(table.schema());
                record.setField(IDENTIFIER_FIELD, (long) i);
                record.setField(DESCRIPTION_FIELD, "record-%d".formatted(i));
                writer.write(record);
            }

            writer.close();
            return writer.toDataFile();
        }
    }

    private List<Record> readRecords(final Table table) throws IOException {
        final List<Record> records = new ArrayList<>();

        try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
            reader.forEach(records::add);
        }

        records.sort((first, second) -> Long.compare((Long) first.getField(IDENTIFIER_FIELD), (Long) second.getField(IDENTIFIER_FIELD)));
        return records;
    }

    private static Path getWarehouseDirectory() {
        try {
            return Files.createTempDirectory("nifi-iceberg-warehouse");
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
