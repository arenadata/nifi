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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processors.iceberg.PutIcebergRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.services.iceberg.hive.cluster.ClusterContext;
import org.apache.nifi.services.iceberg.hive.cluster.ClusterFiles;
import org.apache.nifi.services.iceberg.hive.cluster.IcebergTestCluster;
import org.apache.nifi.services.iceberg.hive.cluster.IcebergTestCluster.Storage;
import org.apache.nifi.services.iceberg.parquet.ParquetIcebergWriter;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.junit.jupiter.EnabledIfDockerAvailable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end test of the Controller Service on a Kerberized cluster with Ranger started by {@link IcebergTestCluster}.
 * Runs with -Diceberg.cluster.test=true and needs host ports 6080, 8020, 9083, 9858, 9859, 9860, 9862, 9866 and 10088.
 */
@EnabledIfDockerAvailable
@EnabledIfSystemProperty(named = "iceberg.cluster.test", matches = "true")
@Timeout(value = 45, unit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IcebergClusterTest {
    private static final String HDFS_NAMESPACE_NAME = "nifi_cluster_hdfs";

    private static final String OZONE_NAMESPACE_NAME = "nifi_cluster_ozone";

    private static final String TABLE_NAME = "records";

    private static final Namespace HDFS_NAMESPACE = Namespace.of(HDFS_NAMESPACE_NAME);

    private static final Namespace OZONE_NAMESPACE = Namespace.of(OZONE_NAMESPACE_NAME);

    private static final TableIdentifier HDFS_TABLE = TableIdentifier.of(HDFS_NAMESPACE, TABLE_NAME);

    private static final TableIdentifier OZONE_TABLE = TableIdentifier.of(OZONE_NAMESPACE, TABLE_NAME);

    private static final String NAMESPACE_LOCATION_FORMAT = "%s/%s.db";

    private static final String TABLE_LOCATION_FORMAT = "%s/%s";

    private static final String HDFS_NAMESPACE_LOCATION = NAMESPACE_LOCATION_FORMAT.formatted(IcebergTestCluster.HDFS_WAREHOUSE, HDFS_NAMESPACE_NAME);

    private static final String OZONE_NAMESPACE_LOCATION = NAMESPACE_LOCATION_FORMAT.formatted(IcebergTestCluster.OZONE_WAREHOUSE, OZONE_NAMESPACE_NAME);

    private static final String LOCATION_PROPERTY = "location";

    private static final String IDENTIFIER_FIELD = "id";

    private static final String DESCRIPTION_FIELD = "description";

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, IDENTIFIER_FIELD, Types.LongType.get()),
            Types.NestedField.optional(2, DESCRIPTION_FIELD, Types.StringType.get())
    );

    private static final String USER_SERVICE_ID = "kerberos-user-service";

    private static final String CATALOG_SERVICE_ID = "hive-metastore-catalog";

    private static final String WRITER_SERVICE_ID = "parquet-iceberg-writer";

    private static final String READER_SERVICE_ID_FORMAT = "record-reader-%d";

    private static final String CATALOG_PROPERTY = "Iceberg Catalog";

    private static final String WRITER_PROPERTY = "Iceberg Writer";

    private static final String READER_PROPERTY = "Record Reader";

    private static final String NAMESPACE_PROPERTY = "Namespace";

    private static final String TABLE_NAME_PROPERTY = "Table Name";

    private static final String SUCCESS_RELATIONSHIP = "success";

    private static final String FAILURE_RELATIONSHIP = "failure";

    private static final List<String> PERMISSION_DENIED_MESSAGES = List.of("Permission denied", "AccessControlException", "PERMISSION_DENIED", "doesn't have");

    private static final Pattern NIFI_USER_AUDIT = Pattern.compile("\"reqUser\"\\s*:\\s*\"%s\"".formatted(IcebergTestCluster.NIFI_USER));

    private static final String AUDIT_RESULT_FORMAT = "\"result\"\\s*:\\s*%d\\b";

    private static final int ALLOWED = 1;

    private static final int DENIED = 0;

    private static final Duration AUDIT_TIMEOUT = Duration.ofMinutes(2);

    private static final AtomicInteger SERVICE_COUNTER = new AtomicInteger();

    private static final List<Row> HDFS_ROWS = new ArrayList<>();

    private static final List<Row> OZONE_ROWS = new ArrayList<>();

    private static IcebergTestCluster cluster;

    private final List<ControllerService> services = new ArrayList<>();

    private TestRunner runner;

    @BeforeAll
    @Timeout(value = 45, unit = TimeUnit.MINUTES)
    static void startCluster() throws Exception {
        cluster = new IcebergTestCluster();
        cluster.start();
    }

    @AfterAll
    static void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(PutIcebergRecord.class);
    }

    @AfterEach
    void disableServices() {
        for (int index = services.size() - 1; index >= 0; index--) {
            final ControllerService service = services.get(index);
            if (runner.isControllerServiceEnabled(service)) {
                runner.disableControllerService(service);
            }
        }
    }

    @Test
    @Order(1)
    void testNamespacesOwnedByKerberosPrincipal() throws Exception {
        final HiveMetastoreCatalog catalog = enableCatalog(ClusterContext.NIFI_KEYTAB).getHiveCatalog();

        catalog.createNamespace(HDFS_NAMESPACE, Map.of(LOCATION_PROPERTY, HDFS_NAMESPACE_LOCATION));
        catalog.createNamespace(OZONE_NAMESPACE, Map.of(LOCATION_PROPERTY, OZONE_NAMESPACE_LOCATION));

        assertEquals(IcebergTestCluster.NIFI_USER, getOwner(catalog, HDFS_NAMESPACE));
        assertEquals(IcebergTestCluster.NIFI_USER, getOwner(catalog, OZONE_NAMESPACE));
    }

    @Test
    @Order(2)
    void testRecordsWrittenByNiFi() throws Exception {
        final HiveMetastoreCatalog catalog = enableCatalog(ClusterContext.NIFI_KEYTAB).getHiveCatalog();
        catalog.createTable(HDFS_TABLE, SCHEMA, PartitionSpec.unpartitioned(), TABLE_LOCATION_FORMAT.formatted(HDFS_NAMESPACE_LOCATION, TABLE_NAME), Map.of());
        catalog.createTable(OZONE_TABLE, SCHEMA, PartitionSpec.unpartitioned(), TABLE_LOCATION_FORMAT.formatted(OZONE_NAMESPACE_LOCATION, TABLE_NAME), Map.of());
        setPutIcebergRecord();

        putRecords(HDFS_TABLE, HDFS_ROWS, List.of(new Row(0, "nifi-0"), new Row(1, "nifi-1")));
        putRecords(OZONE_TABLE, OZONE_ROWS, List.of(new Row(0, "nifi-0"), new Row(1, "nifi-1")));

        assertRows(catalog, HDFS_TABLE, HDFS_ROWS);
        assertRows(catalog, OZONE_TABLE, OZONE_ROWS);
    }

    @Test
    @Order(3)
    void testRangerAuditRecordsNiFiAccess() throws Exception {
        awaitAudit(Storage.HDFS, HDFS_NAMESPACE_NAME, ALLOWED);
        if (IcebergTestCluster.OZONE_RANGER_ENABLED) {
            awaitAudit(Storage.OZONE, OZONE_NAMESPACE_NAME, ALLOWED);
        }
    }

    @Test
    @Order(4)
    void testRangerHdfsPolicyDeniesAndAllowsWrite() throws Exception {
        assertRangerPolicyDeniesAndAllowsWrite(Storage.HDFS, HDFS_TABLE, HDFS_ROWS);
    }

    @Test
    @Order(5)
    void testRangerOzonePolicyDeniesAndAllowsWrite() throws Exception {
        assumeTrue(IcebergTestCluster.OZONE_RANGER_ENABLED, "Ozone Ranger plugin disabled");
        assertRangerPolicyDeniesAndAllowsWrite(Storage.OZONE, OZONE_TABLE, OZONE_ROWS);
    }

    @Test
    @Order(6)
    void testKerberosUserServiceRequiredForKerberosConfiguration() throws InitializationException {
        final HiveMetastoreIcebergCatalog service = new HiveMetastoreIcebergCatalog();
        runner.addControllerService(CATALOG_SERVICE_ID, service);
        services.add(service);
        runner.setProperty(service, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, cluster.clientSite().toString());

        runner.assertNotValid(service);
    }

    @Test
    @Order(7)
    void testKeytabOfAnotherPrincipalFailsEnable() throws InitializationException {
        final HiveMetastoreIcebergCatalog service = addCatalog(ClusterContext.HIVE_KEYTAB);

        assertThrows(AssertionError.class, () -> runner.enableControllerService(service));
        assertFalse(runner.isControllerServiceEnabled(service));
    }

    private void assertRangerPolicyDeniesAndAllowsWrite(final Storage storage, final TableIdentifier table, final List<Row> expectedRows) throws Exception {
        final HiveMetastoreCatalog catalog = enableCatalog(ClusterContext.NIFI_KEYTAB).getHiveCatalog();
        setPutIcebergRecord();

        cluster.revokeNifiWrite(storage);
        try {
            runPutIcebergRecord(table, List.of(new Row(4, "denied-4")));
            assertEquals(1, runner.getFlowFilesForRelationship(FAILURE_RELATIONSHIP).size(), () -> runner.getLogger().getErrorMessages().toString());
            final List<LogMessage> errors = runner.getLogger().getErrorMessages();
            assertTrue(errors.stream().anyMatch(IcebergClusterTest::isPermissionDenied), errors::toString);
            awaitAudit(storage, table.namespace().level(0), DENIED);
        } finally {
            cluster.grantNifiWrite(storage);
        }

        putRecords(table, expectedRows, List.of(new Row(5, "nifi-5")));
        assertRows(catalog, table, expectedRows);
    }

    private HiveMetastoreIcebergCatalog enableCatalog(final String keytab) throws InitializationException {
        final HiveMetastoreIcebergCatalog service = addCatalog(keytab);
        runner.enableControllerService(service);
        return service;
    }

    private HiveMetastoreIcebergCatalog addCatalog(final String keytab) throws InitializationException {
        final KerberosUserService userService = new KeytabUserService(ClusterContext.NIFI_PRINCIPAL, cluster.keytab(keytab).toString());
        runner.addControllerService(USER_SERVICE_ID, userService);
        services.add(userService);
        runner.enableControllerService(userService);

        final HiveMetastoreIcebergCatalog service = new HiveMetastoreIcebergCatalog();
        runner.addControllerService(CATALOG_SERVICE_ID, service);
        services.add(service);
        runner.setProperty(service, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, cluster.clientSite().toString());
        runner.setProperty(service, HiveMetastoreIcebergCatalog.KERBEROS_USER_SERVICE, USER_SERVICE_ID);
        return service;
    }

    private void setPutIcebergRecord() throws InitializationException {
        final ParquetIcebergWriter icebergWriter = new ParquetIcebergWriter();
        runner.addControllerService(WRITER_SERVICE_ID, icebergWriter);
        services.add(icebergWriter);
        runner.enableControllerService(icebergWriter);

        runner.setProperty(CATALOG_PROPERTY, CATALOG_SERVICE_ID);
        runner.setProperty(WRITER_PROPERTY, WRITER_SERVICE_ID);
    }

    private void putRecords(final TableIdentifier table, final List<Row> expectedRows, final List<Row> rows) throws InitializationException {
        runPutIcebergRecord(table, rows);
        assertEquals(1, runner.getFlowFilesForRelationship(SUCCESS_RELATIONSHIP).size(), () -> runner.getLogger().getErrorMessages().toString());
        expectedRows.addAll(rows);
    }

    private void runPutIcebergRecord(final TableIdentifier table, final List<Row> rows) throws InitializationException {
        final MockRecordParser recordReader = new MockRecordParser();
        recordReader.addSchemaField(IDENTIFIER_FIELD, RecordFieldType.LONG);
        recordReader.addSchemaField(DESCRIPTION_FIELD, RecordFieldType.STRING);
        rows.forEach(row -> recordReader.addRecord(row.id(), row.description()));

        final String readerServiceId = READER_SERVICE_ID_FORMAT.formatted(SERVICE_COUNTER.incrementAndGet());
        runner.addControllerService(readerServiceId, recordReader);
        services.add(recordReader);
        runner.enableControllerService(recordReader);

        runner.setProperty(READER_PROPERTY, readerServiceId);
        runner.setProperty(NAMESPACE_PROPERTY, table.namespace().level(0));
        runner.setProperty(TABLE_NAME_PROPERTY, table.name());
        runner.clearTransferState();
        runner.enqueue(new byte[0]);
        runner.run();
    }

    private static void assertRows(final HiveMetastoreCatalog catalog, final TableIdentifier table, final List<Row> expectedRows) throws IOException {
        final List<Row> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(catalog.loadTable(table)).build()) {
            records.forEach(record -> rows.add(new Row((Long) record.getField(IDENTIFIER_FIELD), (String) record.getField(DESCRIPTION_FIELD))));
        }
        rows.sort(Comparator.comparingLong(Row::id));

        assertEquals(expectedRows.stream().sorted(Comparator.comparingLong(Row::id)).toList(), rows, table::toString);
    }

    private static void awaitAudit(final Storage storage, final String resource, final int result) throws InterruptedException {
        final Pattern resultPattern = Pattern.compile(AUDIT_RESULT_FORMAT.formatted(result));
        ClusterFiles.waitUntil("Ranger %s audit for [%s] result [%d]".formatted(storage, resource, result), AUDIT_TIMEOUT, () -> {
            final List<String> lines = storage == Storage.HDFS ? cluster.hdfs().auditLines() : cluster.ozone().auditLines();
            final boolean found = lines.stream().anyMatch(line ->
                    line.contains(resource) && NIFI_USER_AUDIT.matcher(line).find() && resultPattern.matcher(line).find());
            if (!found) {
                throw new IllegalStateException("Audit event not found in %d lines".formatted(lines.size()));
            }
        });
    }

    private static String getOwner(final HiveMetastoreCatalog catalog, final Namespace namespace) throws Exception {
        return catalog.getClientPool().run(client -> client.getDatabase(namespace.level(0)).getOwnerName());
    }

    private static boolean isPermissionDenied(final LogMessage message) {
        Throwable cause = message.getThrowable();
        while (cause != null) {
            final String description = cause.toString();
            if (PERMISSION_DENIED_MESSAGES.stream().anyMatch(description::contains)) {
                return true;
            }
            cause = cause.getCause() == cause ? null : cause.getCause();
        }
        return false;
    }

    private record Row(long id, String description) {
    }

    private static class KeytabUserService extends AbstractControllerService implements KerberosUserService {
        private final String principal;

        private final String keytab;

        private KeytabUserService(final String principal, final String keytab) {
            this.principal = principal;
            this.keytab = keytab;
        }

        @Override
        public KerberosUser createKerberosUser() {
            return new KerberosKeytabUser(principal, keytab);
        }
    }
}
