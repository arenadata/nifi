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
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
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
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.EnabledIfDockerAvailable;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iceberg.FileFormat.PARQUET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Writes Iceberg Data Files to Apache Ozone with the ofs File System and the Ozone client of the include-hadoop-ozone NAR profile.
 * The ofs client writes blocks to the DataNode directly, so the Ozone container uses the hostname localhost, which the DataNode
 * registers, and the Ozone Manager and DataNode ports are bound to the same host ports. The Hive Metastore checks the table location, so it runs in the network namespace
 * of the Ozone container and loads the shaded Ozone File System library copied to the build directory. The Ozone container uses
 * the configuration of the all-in-one image with the Ozone Manager listening on all interfaces, as the image binds it to localhost. Skipped when Docker is not available.
 */
@Testcontainers
@EnabledIfDockerAvailable
class HiveMetastoreIcebergCatalogOfsTest {
    private static final String OZONE_IMAGE_PROPERTY = "ozone.image";

    private static final String OZONE_IMAGE_DEFAULT = "apache/ozone:2.1.2-all-in-one";

    private static final String METASTORE_IMAGE_PROPERTY = "hive.metastore.image";

    private static final String METASTORE_IMAGE_DEFAULT = "apache/hive:4.2.1";

    private static final int OZONE_MANAGER_PORT = 9862;

    private static final int DATA_NODE_RATIS_PORT = 9858;

    private static final int DATA_NODE_STANDALONE_PORT = 9859;

    private static final int METASTORE_PORT = 9083;

    private static final String PORT_BINDING_FORMAT = "%d:%d";

    private static final String NETWORK_MODE_FORMAT = "container:%s";

    private static final String OZONE_HOSTNAME = "localhost";

    private static final String OZONE_READY_PATTERN = ".*Ozone is ready.*\\n";

    private static final String OZONE_CONTAINER_CONFIGURATION = "ozone-container-site.xml";

    private static final String OZONE_CONFIGURATION_PATH = "/etc/hadoop/ozone-site.xml";

    private static final int READABLE_FILE_MODE = Integer.parseInt("644", 8);

    private static final String OZONE_MANAGER_FAILOVER_ATTEMPTS_PROPERTY = "ozone.client.failover.max.attempts";

    private static final String OZONE_MANAGER_FAILOVER_ATTEMPTS = "5";

    private static final String OZONE_REPLICATION_PROPERTY = "ozone.replication";

    private static final String OZONE_REPLICATION = "1";

    private static final String OZONE_REPLICATION_TYPE_PROPERTY = "ozone.replication.type";

    private static final String OZONE_REPLICATION_TYPE = "RATIS";

    private static final String METASTORE_URI_FORMAT = "thrift://%s:%d";

    private static final String FILE_SYSTEM_URI = "ofs://localhost:%d".formatted(OZONE_MANAGER_PORT);

    private static final String VOLUME = "nifi";

    private static final String BUCKET = "iceberg";

    private static final String TABLE_LOCATION_FORMAT = "%s/%s/%s/warehouse/%s/%s";

    private static final String OZONE_FILE_SYSTEM_LIBRARY = "ozone-filesystem-hadoop3.jar";

    private static final String OZONE_FILE_SYSTEM_LIBRARY_PATH = "target/ozone-filesystem/" + OZONE_FILE_SYSTEM_LIBRARY;

    private static final String METASTORE_AUXILIARY_JARS_DIRECTORY = "/opt/hive/ozone-lib";

    private static final String METASTORE_CONFIGURATION_DIRECTORY = "/opt/hive/custom-conf";

    private static final String OZONE_FILE_SYSTEM_PROPERTY = "fs.ofs.impl";

    private static final String OZONE_FILE_SYSTEM_CLASS = "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem";

    private static final String DATA_NODE_HOSTNAME_PROPERTY = "hdds.datanode.use.datanode.hostname";

    private static final String METASTORE_URIS_PROPERTY = "hive.metastore.uris";

    private static final String METASTORE_CONNECT_RETRIES_PROPERTY = "hive.metastore.connect.retries";

    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(5);

    private static final Duration POLL_INTERVAL = Duration.ofSeconds(1);

    private static final String SERVICE_ID = "hive-metastore-catalog";

    private static final String NAMESPACE_FORMAT = "nifi_ofs_%d";

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
            .withCreateContainerCmdModifier(command -> command.withHostName(OZONE_HOSTNAME))
            .withCopyToContainer(MountableFile.forClasspathResource(OZONE_CONTAINER_CONFIGURATION, READABLE_FILE_MODE), OZONE_CONFIGURATION_PATH)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("ozone")))
            .waitingFor(Wait.forLogMessage(OZONE_READY_PATTERN, 1).withStartupTimeout(STARTUP_TIMEOUT));

    static {
        OZONE.setPortBindings(List.of(
                PORT_BINDING_FORMAT.formatted(OZONE_MANAGER_PORT, OZONE_MANAGER_PORT),
                PORT_BINDING_FORMAT.formatted(DATA_NODE_RATIS_PORT, DATA_NODE_RATIS_PORT),
                PORT_BINDING_FORMAT.formatted(DATA_NODE_STANDALONE_PORT, DATA_NODE_STANDALONE_PORT),
                PORT_BINDING_FORMAT.formatted(METASTORE_PORT, METASTORE_PORT)
        ));
    }

    private static GenericContainer<?> metastore;

    @TempDir
    private java.nio.file.Path tempDirectory;

    private TestRunner runner;

    private HiveMetastoreIcebergCatalog catalogService;

    private Namespace namespace;

    private TableIdentifier tableIdentifier;

    private String tableLocation;

    private Configuration configuration;

    @BeforeAll
    static void startServices() throws Exception {
        assertTrue(OZONE.isRunning(), () -> "Ozone container stopped: %s".formatted(OZONE.getLogs()));
        execInOzone("ozone", "sh", "volume", "create", "/" + VOLUME);
        execInOzone("ozone", "sh", "bucket", "create", "/%s/%s".formatted(VOLUME, BUCKET));

        final java.nio.file.Path localFileSystemLibrary = java.nio.file.Path.of(OZONE_FILE_SYSTEM_LIBRARY_PATH);
        assertTrue(Files.isRegularFile(localFileSystemLibrary), () -> "Ozone File System library not found: %s".formatted(localFileSystemLibrary.toAbsolutePath()));

        metastore = new GenericContainer<>(DockerImageName.parse(System.getProperty(METASTORE_IMAGE_PROPERTY, METASTORE_IMAGE_DEFAULT)))
                .withEnv("SERVICE_NAME", "metastore")
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("metastore")))
                .withEnv("HIVE_AUX_JARS_PATH", METASTORE_AUXILIARY_JARS_DIRECTORY)
                .withEnv("HIVE_CUSTOM_CONF_DIR", METASTORE_CONFIGURATION_DIRECTORY)
                .withCopyToContainer(MountableFile.forHostPath(localFileSystemLibrary, READABLE_FILE_MODE), METASTORE_AUXILIARY_JARS_DIRECTORY + "/" + OZONE_FILE_SYSTEM_LIBRARY)
                .withCopyToContainer(Transferable.of(getConfiguration(Map.of(OZONE_FILE_SYSTEM_PROPERTY, OZONE_FILE_SYSTEM_CLASS))),
                        METASTORE_CONFIGURATION_DIRECTORY + "/" + CONFIGURATION_FILE)
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withNetworkMode(NETWORK_MODE_FORMAT.formatted(OZONE.getContainerId())));
        metastore.start();

        final Configuration metastoreConfiguration = new Configuration();
        metastoreConfiguration.set(METASTORE_URIS_PROPERTY, getMetastoreUri());
        metastoreConfiguration.set(METASTORE_CONNECT_RETRIES_PROPERTY, "1");
        final long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
        while (true) {
            try (HiveClientPool clientPool = new HiveClientPool(1, metastoreConfiguration)) {
                clientPool.run(IMetaStoreClient::getAllDatabases);
                return;
            } catch (final Exception e) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("Hive Metastore not available", e);
                }
                Thread.sleep(POLL_INTERVAL.toMillis());
            }
        }
    }

    @AfterAll
    static void stopMetastore() {
        if (metastore != null) {
            metastore.stop();
        }
    }

    @BeforeEach
    void setCatalogService() throws InitializationException, IOException {
        namespace = Namespace.of(NAMESPACE_FORMAT.formatted(NAMESPACE_COUNTER.incrementAndGet()));
        tableIdentifier = TableIdentifier.of(namespace, TABLE_NAME);
        tableLocation = TABLE_LOCATION_FORMAT.formatted(FILE_SYSTEM_URI, VOLUME, BUCKET, namespace.level(0), TABLE_NAME);

        final java.nio.file.Path configurationFile = tempDirectory.resolve(CONFIGURATION_FILE);
        Files.writeString(configurationFile, getConfiguration(Map.of(
                DATA_NODE_HOSTNAME_PROPERTY, Boolean.TRUE.toString(),
                OZONE_MANAGER_FAILOVER_ATTEMPTS_PROPERTY, OZONE_MANAGER_FAILOVER_ATTEMPTS,
                OZONE_REPLICATION_PROPERTY, OZONE_REPLICATION,
                OZONE_REPLICATION_TYPE_PROPERTY, OZONE_REPLICATION_TYPE
        )));

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
            final HiveMetastoreCatalog catalog = catalogService.getHiveCatalog();
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
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void testDataFilesWrittenWithOzoneFileSystem() throws IOException {
        runner.enableControllerService(catalogService);
        final HiveMetastoreCatalog catalog = catalogService.getHiveCatalog();
        assertEquals(OZONE_FILE_SYSTEM_CLASS, catalog.getConf().get(OZONE_FILE_SYSTEM_PROPERTY));

        catalog.createNamespace(namespace, Map.of(NAMESPACE_LOCATION_PROPERTY, tempDirectory.toUri().toString()));
        final Table table = catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned(), tableLocation, Map.of());

        assertInstanceOf(HadoopFileIO.class, table.io());
        assertEquals(tableLocation, table.location());

        final DataFile dataFile = writeDataFile(table);
        table.newAppend().appendFile(dataFile).commit();

        try (FileSystem fileSystem = FileSystem.get(URI.create(FILE_SYSTEM_URI), configuration)) {
            assertEquals(OZONE_FILE_SYSTEM_CLASS, fileSystem.getClass().getName());
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

    private static String execInOzone(final String... command) throws IOException, InterruptedException {
        final GenericContainer.ExecResult result = OZONE.execInContainer(command);
        assertEquals(0, result.getExitCode(), () -> "%s failed: %s".formatted(String.join(" ", command), result.getStderr()));
        return result.getStdout();
    }

    private static String getConfiguration(final Map<String, String> properties) {
        final Map<String, String> orderedProperties = new LinkedHashMap<>(properties);
        final StringBuilder configurationProperties = new StringBuilder();
        orderedProperties.forEach((name, value) -> configurationProperties.append(PROPERTY_FORMAT.formatted(name, value)));
        return CONFIGURATION_FORMAT.formatted(configurationProperties);
    }

    private static String getMetastoreUri() {
        return METASTORE_URI_FORMAT.formatted(OZONE.getHost(), METASTORE_PORT);
    }
}
