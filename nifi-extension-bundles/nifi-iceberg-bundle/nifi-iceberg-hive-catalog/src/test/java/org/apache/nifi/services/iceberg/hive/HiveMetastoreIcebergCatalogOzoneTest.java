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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.EnabledIfDockerAvailable;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iceberg.FileFormat.PARQUET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Writes Iceberg Data Files to Apache Ozone through Hadoop File IO. Storage is reached over the Ozone S3 Gateway with
 * the s3a File System, because the S3 Gateway proxies data through a single endpoint, while the ofs File System requires
 * the client to address DataNodes directly, which container port mapping does not support. The Hive Metastore container
 * records the table location and never reads it. Skipped when Docker is not available.
 */
@Testcontainers
@EnabledIfDockerAvailable
class HiveMetastoreIcebergCatalogOzoneTest {
    private static final String OZONE_IMAGE_PROPERTY = "ozone.image";

    private static final String OZONE_IMAGE_DEFAULT = "apache/ozone:2.1.2-all-in-one";

    private static final String METASTORE_IMAGE_PROPERTY = "hive.metastore.image";

    private static final String METASTORE_IMAGE_DEFAULT = "apache/hive:4.0.1";

    private static final int S3_GATEWAY_PORT = 9878;

    private static final int METASTORE_PORT = 9083;

    private static final String METASTORE_URI_FORMAT = "thrift://%s:%d";

    private static final String ENDPOINT_FORMAT = "http://%s:%d";

    private static final String BUCKET = "nifi-iceberg";

    private static final String BUCKET_PATH_FORMAT = "/s3v/%s";

    private static final String FILE_SYSTEM_URI_FORMAT = "s3a://%s";

    private static final String TABLE_LOCATION_FORMAT = "%s/warehouse/%s/%s";

    private static final String ACCESS_KEY = "nifi";

    private static final String SECRET_KEY = "nifi-secret";

    private static final String SERVICE_ID = "hive-metastore-catalog";

    private static final String NAMESPACE_FORMAT = "nifi_ozone_%d";

    private static final String TABLE_NAME = "records";

    private static final String NAMESPACE_LOCATION_PROPERTY = "location";

    private static final String DATA_FILE_FORMAT = "%s/data/%d.parquet";

    private static final String METADATA_DIRECTORY_FORMAT = "%s/metadata";

    private static final String CONFIGURATION_FILE = "core-site.xml";

    private static final String CONFIGURATION_FORMAT = """
            <?xml version="1.0" encoding="UTF-8"?>
            <configuration>%s</configuration>""";

    private static final String PROPERTY_FORMAT = "<property><name>%s</name><value>%s</value></property>";

    private static final String IDENTIFIER_FIELD = "id";

    private static final String DESCRIPTION_FIELD = "description";

    private static final String DESCRIPTION_FORMAT = "record-%d";

    private static final int RECORD_COUNT = 2;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, IDENTIFIER_FIELD, Types.LongType.get()),
            Types.NestedField.optional(2, DESCRIPTION_FIELD, Types.StringType.get())
    );

    private static final AtomicInteger NAMESPACE_COUNTER = new AtomicInteger();

    @Container
    private static final GenericContainer<?> OZONE = new GenericContainer<>(DockerImageName.parse(System.getProperty(OZONE_IMAGE_PROPERTY, OZONE_IMAGE_DEFAULT)))
            .withExposedPorts(S3_GATEWAY_PORT)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)));

    @Container
    private static final GenericContainer<?> METASTORE = new GenericContainer<>(DockerImageName.parse(System.getProperty(METASTORE_IMAGE_PROPERTY, METASTORE_IMAGE_DEFAULT)))
            .withEnv("SERVICE_NAME", "metastore")
            .withExposedPorts(METASTORE_PORT)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)));

    @TempDir
    private java.nio.file.Path tempDirectory;

    private TestRunner runner;

    private HiveMetastoreIcebergCatalog catalogService;

    private Namespace namespace;

    private TableIdentifier tableIdentifier;

    private String tableLocation;

    private Configuration configuration;

    @BeforeAll
    static void createBucket() throws IOException, InterruptedException {
        final GenericContainer.ExecResult result = OZONE.execInContainer("ozone", "sh", "bucket", "create", BUCKET_PATH_FORMAT.formatted(BUCKET));
        assertEquals(0, result.getExitCode(), result.getStderr());
    }

    @BeforeEach
    void setCatalogService() throws InitializationException, IOException {
        namespace = Namespace.of(NAMESPACE_FORMAT.formatted(NAMESPACE_COUNTER.incrementAndGet()));
        tableIdentifier = TableIdentifier.of(namespace, TABLE_NAME);
        tableLocation = TABLE_LOCATION_FORMAT.formatted(FILE_SYSTEM_URI_FORMAT.formatted(BUCKET), namespace.level(0), TABLE_NAME);

        final Map<String, String> properties = new LinkedHashMap<>();
        properties.put("fs.s3a.endpoint", ENDPOINT_FORMAT.formatted(OZONE.getHost(), OZONE.getMappedPort(S3_GATEWAY_PORT)));
        properties.put("fs.s3a.endpoint.region", "us-east-1");
        properties.put("fs.s3a.path.style.access", "true");
        properties.put("fs.s3a.access.key", ACCESS_KEY);
        properties.put("fs.s3a.secret.key", SECRET_KEY);
        properties.put("fs.s3a.bucket.probe", "0");
        properties.put("fs.s3a.change.detection.mode", "none");
        final java.nio.file.Path configurationFile = writeConfiguration(properties);

        configuration = new Configuration();
        configuration.addResource(new Path(configurationFile.toString()));

        catalogService = new HiveMetastoreIcebergCatalog();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService(SERVICE_ID, catalogService);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, getMetastoreUri());
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, configurationFile.toString());
    }

    @AfterEach
    void disableCatalogService() {
        if (runner.isControllerServiceEnabled(catalogService)) {
            final HiveCatalog catalog = (HiveCatalog) catalogService.getCatalog();
            if (catalog.tableExists(tableIdentifier)) {
                catalog.dropTable(tableIdentifier, false);
            }
            if (catalog.namespaceExists(namespace)) {
                catalog.dropNamespace(namespace);
            }
            runner.disableControllerService(catalogService);
        }
    }

    @Test
    void testDataFilesWrittenToObjectStore() throws IOException {
        runner.enableControllerService(catalogService);
        final HiveCatalog catalog = assertInstanceOf(HiveCatalog.class, catalogService.getCatalog());

        catalog.createNamespace(namespace, Map.of(NAMESPACE_LOCATION_PROPERTY, tempDirectory.toUri().toString()));
        final Table table = catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned(), tableLocation, Map.of());

        assertInstanceOf(HadoopFileIO.class, table.io());
        assertEquals(tableLocation, table.location());

        final DataFile dataFile = writeDataFile(table);
        table.newAppend().appendFile(dataFile).commit();

        try (FileSystem fileSystem = FileSystem.get(URI.create(FILE_SYSTEM_URI_FORMAT.formatted(BUCKET)), configuration)) {
            assertTrue(fileSystem.exists(new Path(dataFile.location())), dataFile.location());
            assertTrue(fileSystem.exists(new Path(METADATA_DIRECTORY_FORMAT.formatted(tableLocation))), tableLocation);
        }

        final Table committed = catalog.loadTable(tableIdentifier);
        assertNotNull(committed.currentSnapshot());
        assertEquals(RECORD_COUNT, readRecords(committed).size());
    }

    private DataFile writeDataFile(final Table table) throws IOException {
        final OutputFile outputFile = table.io().newOutputFile(DATA_FILE_FORMAT.formatted(table.location(), System.nanoTime()));
        final GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema());

        try (DataWriter<Record> writer = appenderFactory.newDataWriter(EncryptedFiles.plainAsEncryptedOutput(outputFile), PARQUET, null)) {
            for (int i = 0; i < RECORD_COUNT; i++) {
                final GenericRecord record = GenericRecord.create(table.schema());
                record.setField(IDENTIFIER_FIELD, (long) i);
                record.setField(DESCRIPTION_FIELD, DESCRIPTION_FORMAT.formatted(i));
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

        return records;
    }

    private java.nio.file.Path writeConfiguration(final Map<String, String> properties) throws IOException {
        final StringBuilder configurationProperties = new StringBuilder();
        properties.forEach((name, value) -> configurationProperties.append(PROPERTY_FORMAT.formatted(name, value)));

        final java.nio.file.Path configurationFile = tempDirectory.resolve(CONFIGURATION_FILE);
        Files.writeString(configurationFile, CONFIGURATION_FORMAT.formatted(configurationProperties));
        return configurationFile;
    }

    private String getMetastoreUri() {
        return METASTORE_URI_FORMAT.formatted(METASTORE.getHost(), METASTORE.getMappedPort(METASTORE_PORT));
    }
}
