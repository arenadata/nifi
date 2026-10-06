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
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.types.Types;
import org.apache.nifi.processors.iceberg.PutIcebergRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.services.iceberg.parquet.ParquetIcebergWriter;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.EnabledIfDockerAvailable;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Writes records with PutIcebergRecord through the Controller Service and reads them with Trino, which resolves the
 * table through the same Hive Metastore and reads the Data Files from HDFS. All servers share the network namespace of
 * the NameNode container, so that hdfs://localhost and the DataNode address registered as localhost resolve the same way
 * for the test client on the host, through published ports, and for the Metastore and Trino inside the namespace.
 * Skipped when Docker is not available. Images can be replaced using -Dhadoop.image, -Dhive.metastore.image and
 * -Dtrino.image.
 */
@Testcontainers
@EnabledIfDockerAvailable
@Timeout(value = 15, unit = TimeUnit.MINUTES)
class HiveMetastoreIcebergCatalogTrinoTest {
    private static final String HADOOP_IMAGE_PROPERTY = "hadoop.image";

    private static final String HADOOP_IMAGE_DEFAULT = "apache/hadoop:3.4.1";

    private static final String METASTORE_IMAGE_PROPERTY = "hive.metastore.image";

    private static final String METASTORE_IMAGE_DEFAULT = "apache/hive:4.2.1";

    private static final String TRINO_IMAGE_PROPERTY = "trino.image";

    private static final String TRINO_IMAGE_DEFAULT = "trinodb/trino:483";

    private static final int NAME_NODE_PORT = 8020;

    private static final int DATA_NODE_PORT = 9866;

    private static final int METASTORE_PORT = 9083;

    private static final String FILE_SYSTEM_URI = "hdfs://localhost:%d".formatted(NAME_NODE_PORT);

    private static final String METASTORE_URI_FORMAT = "thrift://%s:%d";

    private static final String NETWORK_URI = "thrift://localhost:%d".formatted(METASTORE_PORT);

    private static final String PORT_BINDING_FORMAT = "%d:%d";

    private static final String NETWORK_MODE_FORMAT = "container:%s";

    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(5);

    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);

    private static final String LIVE_DATA_NODES = "Live datanodes (1)";

    private static final String TRINO_STARTED_PATTERN = ".*SERVER STARTED.*\\n";

    private static final String TRINO_CATALOG_PATH = "/etc/trino/catalog/iceberg.properties";

    private static final String TRINO_HDFS_CONFIGURATION_PATH = "/etc/trino/hdfs-site.xml";

    private static final String TRINO_CATALOG = """
            connector.name=iceberg
            iceberg.catalog.type=hive_metastore
            hive.metastore.uri=%s
            fs.hadoop.enabled=true
            hive.config.resources=%s
            """.formatted(NETWORK_URI, TRINO_HDFS_CONFIGURATION_PATH);

    private static final String CONFIGURATION_FORMAT = """
            <?xml version="1.0" encoding="UTF-8"?>
            <configuration>
                <property><name>fs.defaultFS</name><value>%s</value></property>
                <property><name>dfs.client.use.datanode.hostname</name><value>true</value></property>
                <property><name>dfs.replication</name><value>1</value></property>
            </configuration>""";

    private static final String QUERY_FORMAT = "SELECT id, description FROM iceberg.%s.%s ORDER BY id";

    private static final String SERVICE_ID = "hive-metastore-catalog";

    private static final String WRITER_SERVICE_ID = "parquet-iceberg-writer";

    private static final String READER_SERVICE_ID = "record-reader";

    private static final String CATALOG_PROPERTY = "Iceberg Catalog";

    private static final String WRITER_PROPERTY = "Iceberg Writer";

    private static final String READER_PROPERTY = "Record Reader";

    private static final String NAMESPACE_PROPERTY = "Namespace";

    private static final String TABLE_NAME_PROPERTY = "Table Name";

    private static final String SUCCESS_RELATIONSHIP = "success";

    private static final String NAMESPACE_FORMAT = "nifi_trino_%d";

    private static final String TABLE_NAME = "records";

    private static final String NAMESPACE_LOCATION_PROPERTY = "location";

    private static final String NAMESPACE_LOCATION_FORMAT = "%s/warehouse/%s.db";

    private static final String TABLE_LOCATION_FORMAT = "%s/%s";

    private static final String IDENTIFIER_FIELD = "id";

    private static final String DESCRIPTION_FIELD = "description";

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, IDENTIFIER_FIELD, Types.LongType.get()),
            Types.NestedField.optional(2, DESCRIPTION_FIELD, Types.StringType.get())
    );

    private static final AtomicInteger NAMESPACE_COUNTER = new AtomicInteger();

    @Container
    private static final GenericContainer<?> NAME_NODE = new GenericContainer<>(DockerImageName.parse(System.getProperty(HADOOP_IMAGE_PROPERTY, HADOOP_IMAGE_DEFAULT)))
            .withEnv("ENSURE_NAMENODE_DIR", "/tmp/hadoop-root/dfs/name")
            .withEnv("CORE-SITE.XML_fs.defaultFS", FILE_SYSTEM_URI)
            .withEnv("HDFS-SITE.XML_dfs.replication", "1")
            .withEnv("HDFS-SITE.XML_dfs.permissions.enabled", "false")
            .withEnv("HDFS-SITE.XML_dfs.namenode.rpc-bind-host", "0.0.0.0")
            .withCommand("hdfs", "namenode")
            .waitingFor(Wait.forListeningPorts(NAME_NODE_PORT).withStartupTimeout(STARTUP_TIMEOUT));

    @Container
    private static final GenericContainer<?> DATA_NODE = new GenericContainer<>(DockerImageName.parse(System.getProperty(HADOOP_IMAGE_PROPERTY, HADOOP_IMAGE_DEFAULT)))
            .withEnv("CORE-SITE.XML_fs.defaultFS", FILE_SYSTEM_URI)
            .withEnv("HDFS-SITE.XML_dfs.replication", "1")
            .withEnv("HDFS-SITE.XML_dfs.datanode.hostname", "localhost")
            .withCommand("hdfs", "datanode")
            .withCreateContainerCmdModifier(command -> command.getHostConfig().withNetworkMode(NETWORK_MODE_FORMAT.formatted(NAME_NODE.getContainerId())))
            .dependsOn(NAME_NODE);

    @Container
    private static final GenericContainer<?> METASTORE = new GenericContainer<>(DockerImageName.parse(System.getProperty(METASTORE_IMAGE_PROPERTY, METASTORE_IMAGE_DEFAULT)))
            .withEnv("SERVICE_NAME", "metastore")
            .withCreateContainerCmdModifier(command -> command.getHostConfig().withNetworkMode(NETWORK_MODE_FORMAT.formatted(NAME_NODE.getContainerId())))
            .dependsOn(NAME_NODE);

    @Container
    private static final GenericContainer<?> TRINO = new GenericContainer<>(DockerImageName.parse(System.getProperty(TRINO_IMAGE_PROPERTY, TRINO_IMAGE_DEFAULT)))
            .withCopyToContainer(Transferable.of(TRINO_CATALOG), TRINO_CATALOG_PATH)
            .withCopyToContainer(Transferable.of(CONFIGURATION_FORMAT.formatted(FILE_SYSTEM_URI)), TRINO_HDFS_CONFIGURATION_PATH)
            .withCreateContainerCmdModifier(command -> command.getHostConfig().withNetworkMode(NETWORK_MODE_FORMAT.formatted(NAME_NODE.getContainerId())))
            .waitingFor(Wait.forLogMessage(TRINO_STARTED_PATTERN, 1).withStartupTimeout(STARTUP_TIMEOUT))
            .dependsOn(NAME_NODE, METASTORE);

    static {
        NAME_NODE.setPortBindings(List.of(
                PORT_BINDING_FORMAT.formatted(NAME_NODE_PORT, NAME_NODE_PORT),
                PORT_BINDING_FORMAT.formatted(DATA_NODE_PORT, DATA_NODE_PORT),
                PORT_BINDING_FORMAT.formatted(METASTORE_PORT, METASTORE_PORT)
        ));
    }

    @TempDir
    private Path tempDirectory;

    private TestRunner runner;

    private HiveMetastoreIcebergCatalog catalogService;

    private Namespace namespace;

    private TableIdentifier tableIdentifier;

    @BeforeAll
    static void waitForServices() throws InterruptedException {
        waitUntilAvailable("DataNode not registered with NameNode", () -> {
            final ExecResult result = NAME_NODE.execInContainer("hdfs", "dfsadmin", "-report", "-live");
            if (!result.getStdout().contains(LIVE_DATA_NODES)) {
                throw new IllegalStateException(result.getStdout() + result.getStderr());
            }
        });

        final Configuration metastoreConfiguration = new Configuration();
        metastoreConfiguration.set("hive.metastore.uris", getMetastoreUri());
        metastoreConfiguration.set("hive.metastore.connect.retries", "1");
        waitUntilAvailable("Hive Metastore not available", () -> {
            try (HiveClientPool clientPool = new HiveClientPool(1, metastoreConfiguration)) {
                clientPool.run(IMetaStoreClient::getAllDatabases);
            }
        });
    }

    @BeforeEach
    void setCatalogService() throws InitializationException, IOException {
        namespace = Namespace.of(NAMESPACE_FORMAT.formatted(NAMESPACE_COUNTER.incrementAndGet()));
        tableIdentifier = TableIdentifier.of(namespace, TABLE_NAME);

        final Path configurationFile = tempDirectory.resolve("hdfs-site.xml");
        Files.writeString(configurationFile, CONFIGURATION_FORMAT.formatted(FILE_SYSTEM_URI));

        runner = TestRunners.newTestRunner(PutIcebergRecord.class);
        catalogService = new HiveMetastoreIcebergCatalog();
        runner.addControllerService(SERVICE_ID, catalogService);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, getMetastoreUri());
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, configurationFile.toString());
        runner.enableControllerService(catalogService);
    }

    @AfterEach
    void disableCatalogService() {
        if (runner.isControllerServiceEnabled(catalogService)) {
            final HiveMetastoreCatalog catalog = catalogService.getHiveCatalog();
            if (catalog.tableExists(tableIdentifier)) {
                catalog.dropTable(tableIdentifier, true);
            }
            if (catalog.namespaceExists(namespace)) {
                catalog.dropNamespace(namespace);
            }
            runner.disableControllerService(catalogService);
        }
    }

    @Test
    void testRecordsWrittenByPutIcebergRecordReadByTrino() throws Exception {
        final HiveMetastoreCatalog catalog = catalogService.getHiveCatalog();
        final String namespaceLocation = NAMESPACE_LOCATION_FORMAT.formatted(FILE_SYSTEM_URI, namespace.level(0));
        catalog.createNamespace(namespace, Map.of(NAMESPACE_LOCATION_PROPERTY, namespaceLocation));
        catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned(), TABLE_LOCATION_FORMAT.formatted(namespaceLocation, TABLE_NAME), Map.of());

        final ParquetIcebergWriter icebergWriter = new ParquetIcebergWriter();
        runner.addControllerService(WRITER_SERVICE_ID, icebergWriter);
        runner.enableControllerService(icebergWriter);

        final MockRecordParser recordReader = new MockRecordParser();
        recordReader.addSchemaField(IDENTIFIER_FIELD, RecordFieldType.LONG);
        recordReader.addSchemaField(DESCRIPTION_FIELD, RecordFieldType.STRING);
        recordReader.addRecord(0L, "record-0");
        recordReader.addRecord(1L, "record-1");
        runner.addControllerService(READER_SERVICE_ID, recordReader);
        runner.enableControllerService(recordReader);

        runner.setProperty(CATALOG_PROPERTY, SERVICE_ID);
        runner.setProperty(WRITER_PROPERTY, WRITER_SERVICE_ID);
        runner.setProperty(READER_PROPERTY, READER_SERVICE_ID);
        runner.setProperty(NAMESPACE_PROPERTY, namespace.level(0));
        runner.setProperty(TABLE_NAME_PROPERTY, TABLE_NAME);
        runner.enqueue(new byte[0]);

        runner.run();
        assertEquals(1, runner.getFlowFilesForRelationship(SUCCESS_RELATIONSHIP).size(), () -> runner.getLogger().getErrorMessages().toString());

        final ExecResult result = TRINO.execInContainer("trino", "--output-format", "TSV", "--execute", QUERY_FORMAT.formatted(namespace.level(0), TABLE_NAME));

        assertEquals(0, result.getExitCode(), result.getStderr());
        assertEquals(List.of("0\trecord-0", "1\trecord-1"), result.getStdout().lines().toList(), result.getStdout());
    }

    private static String getMetastoreUri() {
        return METASTORE_URI_FORMAT.formatted(NAME_NODE.getHost(), METASTORE_PORT);
    }

    private static void waitUntilAvailable(final String failureMessage, final AvailabilityCheck check) throws InterruptedException {
        final long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
        while (true) {
            try {
                check.run();
                return;
            } catch (final Exception e) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException(failureMessage, e);
                }
                Thread.sleep(POLL_INTERVAL.toMillis());
            }
        }
    }

    @FunctionalInterface
    private interface AvailabilityCheck {
        void run() throws Exception;
    }
}
