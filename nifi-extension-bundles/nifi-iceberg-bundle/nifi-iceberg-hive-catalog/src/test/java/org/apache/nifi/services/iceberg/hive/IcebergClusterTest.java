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

import org.apache.iceberg.HasTableOperations;
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
import org.apache.nifi.services.iceberg.hive.cluster.AuditEvent;
import org.apache.nifi.services.iceberg.hive.cluster.ClusterContext;
import org.apache.nifi.services.iceberg.hive.cluster.ClusterFiles;
import org.apache.nifi.services.iceberg.hive.cluster.IcebergTestCluster;
import org.apache.nifi.services.iceberg.hive.cluster.IcebergTestCluster.Access;
import org.apache.nifi.services.iceberg.hive.cluster.IcebergTestCluster.Storage;
import org.apache.nifi.services.iceberg.hive.cluster.QueryEngine;
import org.apache.nifi.services.iceberg.hive.cluster.QueryEngine.FailedQuery;
import org.apache.nifi.services.iceberg.hive.cluster.SparkEngine;
import org.apache.nifi.services.iceberg.hive.cluster.TrinoEngine;
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
// import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.junit.jupiter.EnabledIfDockerAvailable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test of the Controller Service, Trino and Spark on a Kerberized cluster with Ranger plugins for HDFS and
 * Ozone started by {@link IcebergTestCluster}. Needs Docker and free host ports from {@link ClusterContext}.
 */
@EnabledIfDockerAvailable
// @EnabledIfSystemProperty(named = "iceberg.cluster.test", matches = "true")
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

    private static final String SECURED_NAMESPACE_FORMAT = "nifi_ranger_%s";

    private static final String DENIED_NAMESPACE_FORMAT = "nifi_denied_%s";

    private static final String TRINO_DENIED_NAMESPACE_FORMAT = "trino_denied_%s";

    private static final String ALPHA_TABLE_NAME = "alpha";

    private static final String BETA_TABLE_NAME = "beta";

    private static final String GAMMA_TABLE_NAME = "gamma";

    private static final String TRINO_CREATED_TABLE_NAME = "trino_created";

    private static final String SPARK_CREATED_TABLE_NAME = "spark_created";

    private static final String NAMESPACE_RESOURCE_FORMAT = "%s.db";

    private static final String TABLE_RESOURCE_FORMAT = "%s.db/%s";

    private static final String DATA_RESOURCE_FORMAT = "%s/data";

    private static final String METADATA_RESOURCE_FORMAT = "%s/metadata/%s";

    private static final String PATH_SEPARATOR = "/";

    private static final String LOCATION_PROPERTY = "location";

    private static final String IDENTIFIER_FIELD = "id";

    private static final String DESCRIPTION_FIELD = "description";

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, IDENTIFIER_FIELD, Types.LongType.get()),
            Types.NestedField.optional(2, DESCRIPTION_FIELD, Types.StringType.get())
    );

    private static final String SELECT_FORMAT = "SELECT %s, %s FROM %s.%s.%s ORDER BY %s";

    private static final String INSERT_FORMAT = "INSERT INTO %s.%s.%s VALUES (%d, '%s')";

    private static final String SNAPSHOTS_COUNT_FORMAT = "SELECT count(*) FROM %s.%s.\"%s$snapshots\"";

    private static final String DROP_TABLE_FORMAT = "DROP TABLE %s.%s.%s";

    private static final String TRINO_CREATE_TABLE_FORMAT = "CREATE TABLE %s.%s.%s (%s bigint, %s varchar)";

    private static final String SPARK_CREATE_TABLE_FORMAT = "CREATE TABLE %s.%s.%s (%s BIGINT, %s STRING) USING iceberg";

    private static final String SHOW_TABLES_FORMAT = "SHOW TABLES FROM %s.%s";

    private static final String CREATE_SCHEMA_FORMAT = "CREATE SCHEMA %s.%s WITH (location = '%s')";

    private static final String SHOW_SCHEMAS_FORMAT = "SHOW SCHEMAS FROM %s";

    private static final String STATEMENT_SEPARATOR = "; ";

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

    private static final String NIFI_USER = IcebergTestCluster.NIFI_USER;

    private static final String TRINO_USER = IcebergTestCluster.TRINO_USER;

    private static final String SPARK_USER = IcebergTestCluster.SPARK_USER;

    private static final String HDFS_DENIED_FORMAT = "Permission denied: user=%s, access=%s";

    private static final String OZONE_DENIED_FORMAT = "User %s doesn't have %s permission";

    private static final String OZONE_DENIED_RESOURCE_FORMAT = OZONE_DENIED_FORMAT + " to access %s";

    private static final String READ_ACCESS = "READ";

    private static final String WRITE_ACCESS = "WRITE";

    private static final String CREATE_ACCESS = "CREATE";

    private static final String DELETE_ACCESS = "DELETE";

    private static final String BUCKET_RESOURCE_TYPE = "bucket";

    private static final String HDFS_DENIED_EXCEPTION = "RangerAccessControlException";

    private static final String EXTERNAL_PATH_FAILED = "Failed to create external path";

    private static final String DELETE_DIRECTORY_FAILED = "Failed to delete directory";

    private static final String DELETE_AUDIT_ACCESS = "delete";

    private static final String FALLBACK_ENFORCER = "hadoop-acl";

    private static final String AUDIT_DESCRIPTION_FORMAT = "for user [%s] resource [%s] result [%d]";

    private static final LongPredicate ANY_POLICY = policyId -> policyId != AuditEvent.NO_POLICY;

    private static final LongPredicate NO_POLICY = policyId -> policyId == AuditEvent.NO_POLICY;

    private static final Duration AUDIT_TIMEOUT = Duration.ofMinutes(2);

    private static final AtomicInteger SERVICE_COUNTER = new AtomicInteger();

    private static final TableRows HDFS_ROWS = new TableRows(HDFS_TABLE, new ArrayList<>());

    private static final TableRows OZONE_ROWS = new TableRows(OZONE_TABLE, new ArrayList<>());

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

        assertEquals(NIFI_USER, getOwner(catalog, HDFS_NAMESPACE));
        assertEquals(NIFI_USER, getOwner(catalog, OZONE_NAMESPACE));
    }

    @Test
    @Order(2)
    void testRecordsWrittenByNiFi() throws Exception {
        final HiveMetastoreCatalog catalog = enableCatalog(ClusterContext.NIFI_KEYTAB).getHiveCatalog();
        catalog.createTable(HDFS_TABLE, SCHEMA, PartitionSpec.unpartitioned(), TABLE_LOCATION_FORMAT.formatted(HDFS_NAMESPACE_LOCATION, TABLE_NAME), Map.of());
        catalog.createTable(OZONE_TABLE, SCHEMA, PartitionSpec.unpartitioned(), TABLE_LOCATION_FORMAT.formatted(OZONE_NAMESPACE_LOCATION, TABLE_NAME), Map.of());
        setPutIcebergRecord();

        putRecords(HDFS_ROWS, List.of(new Row(0, "nifi-0"), new Row(1, "nifi-1")));
        putRecords(OZONE_ROWS, List.of(new Row(0, "nifi-0"), new Row(1, "nifi-1")));

        assertRows(catalog, HDFS_ROWS);
        assertRows(catalog, OZONE_ROWS);
    }

    @Test
    @Order(3)
    void testRecordsWrittenByNiFiReadByTrinoAndSpark() throws Exception {
        assertEngineRows(cluster.trino(), TrinoEngine.CATALOG, HDFS_ROWS, OZONE_ROWS);
        assertEngineRows(cluster.spark(), SparkEngine.CATALOG, HDFS_ROWS, OZONE_ROWS);
    }

    @Test
    @Order(4)
    void testRecordsWrittenBySparkAndTrinoAppendedByNiFi() throws Exception {
        insertRow(cluster.spark(), SparkEngine.CATALOG, new Row(2, "spark-2"), HDFS_ROWS, OZONE_ROWS);
        insertRow(cluster.trino(), TrinoEngine.CATALOG, new Row(3, "trino-3"), HDFS_ROWS, OZONE_ROWS);

        final HiveMetastoreCatalog catalog = enableCatalog(ClusterContext.NIFI_KEYTAB).getHiveCatalog();
        setPutIcebergRecord();
        putRecords(HDFS_ROWS, List.of(new Row(4, "nifi-4")));
        putRecords(OZONE_ROWS, List.of(new Row(4, "nifi-4")));

        assertRows(catalog, HDFS_ROWS);
        assertRows(catalog, OZONE_ROWS);
        assertEngineRows(cluster.trino(), TrinoEngine.CATALOG, HDFS_ROWS, OZONE_ROWS);
        assertEngineRows(cluster.spark(), SparkEngine.CATALOG, HDFS_ROWS, OZONE_ROWS);
    }

    @ParameterizedTest
    @EnumSource(Storage.class)
    @Order(5)
    void testTablesCreatedByTrinoAndSparkAppendedByNiFi(final Storage storage) throws Exception {
        final Namespace namespace = getRows(storage).table().namespace();
        final TableRows trinoTable = new TableRows(TableIdentifier.of(namespace, TRINO_CREATED_TABLE_NAME), new ArrayList<>());
        final TableRows sparkTable = new TableRows(TableIdentifier.of(namespace, SPARK_CREATED_TABLE_NAME), new ArrayList<>());

        final Row trinoRow = new Row(0, "trino-0");
        cluster.trino().query(String.join(STATEMENT_SEPARATOR,
                createTable(TRINO_CREATE_TABLE_FORMAT, TrinoEngine.CATALOG, trinoTable.table()), insert(TrinoEngine.CATALOG, trinoTable.table(), trinoRow)));
        trinoTable.rows().add(trinoRow);
        final Row sparkRow = new Row(1, "spark-1");
        cluster.spark().query(String.join(STATEMENT_SEPARATOR,
                createTable(SPARK_CREATE_TABLE_FORMAT, SparkEngine.CATALOG, sparkTable.table()), insert(SparkEngine.CATALOG, sparkTable.table(), sparkRow)));
        sparkTable.rows().add(sparkRow);

        final HiveMetastoreCatalog catalog = enableCatalog(ClusterContext.NIFI_KEYTAB).getHiveCatalog();
        setPutIcebergRecord();
        putRecords(trinoTable, List.of(new Row(2, "nifi-2")));
        putRecords(sparkTable, List.of(new Row(2, "nifi-2")));

        assertRows(catalog, trinoTable);
        assertRows(catalog, sparkTable);
        assertEngineRows(cluster.spark(), SparkEngine.CATALOG, trinoTable, sparkTable);
        assertEngineRows(cluster.trino(), TrinoEngine.CATALOG, trinoTable, sparkTable);
    }

    @ParameterizedTest
    @EnumSource(Storage.class)
    @Order(6)
    void testRangerAuditRecordsAllowedAccess(final Storage storage) throws Exception {
        final String resource = NAMESPACE_RESOURCE_FORMAT.formatted(getRows(storage).table().namespace().level(0));
        for (final String user : IcebergTestCluster.DATA_USERS) {
            awaitAudit(storage, 0, user, resource, AuditEvent.ALLOWED, ANY_POLICY);
        }

        final List<AuditEvent> fallbackEvents = getAuditEvents(storage, 0).stream().filter(event -> FALLBACK_ENFORCER.equals(event.enforcer())).toList();
        assertTrue(fallbackEvents.isEmpty(), fallbackEvents::toString);
    }

    @ParameterizedTest
    @EnumSource(Storage.class)
    @Order(7)
    void testRangerPoliciesDecideWarehouseWrites(final Storage storage) throws Exception {
        final TableRows table = getRows(storage);
        final String namespaceName = table.table().namespace().level(0);
        final String tablePath = TABLE_RESOURCE_FORMAT.formatted(namespaceName, TABLE_NAME);
        final HiveMetastoreCatalog catalog = enableCatalog(ClusterContext.NIFI_KEYTAB).getHiveCatalog();
        setPutIcebergRecord();

        final String tableResource = IcebergTestCluster.warehouseResource(storage, tablePath);
        final Set<Long> policies = new LinkedHashSet<>();
        try {
            final long tableDeny = cluster.deny(storage, tableResource, TRINO_USER, Access.WRITE);
            policies.add(tableDeny);
            policies.add(cluster.deny(storage, tableResource, SPARK_USER, Access.WRITE));
            cluster.revokeNifiWrite(storage);
            cluster.awaitPolicyRefresh(storage);
            final int offset = cluster.auditLines(storage).size();

            assertNiFiDenied(table.table(), List.of(new Row(5, "denied-5")), getNiFiDeniedMarkers(storage, WRITE_ACCESS, CREATE_ACCESS));
            final FailedQuery trinoQuery = cluster.trino().queryFailure(String.join(STATEMENT_SEPARATOR,
                    select(TrinoEngine.CATALOG, table.table()), insert(TrinoEngine.CATALOG, table.table(), new Row(5, "trino-5"))));
            assertEquals(toValues(table.rows()), withoutBlankLines(trinoQuery.rows()), trinoQuery::output);
            assertOutputDenied(trinoQuery, getDeniedMessage(storage, TRINO_USER, WRITE_ACCESS, CREATE_ACCESS));
            final FailedQuery sparkQuery = cluster.spark().queryFailure(String.join(STATEMENT_SEPARATOR,
                    select(SparkEngine.CATALOG, table.table()), insert(SparkEngine.CATALOG, table.table(), new Row(5, "spark-5"))));
            assertEquals(toValues(table.rows()), withoutBlankLines(sparkQuery.rows()), sparkQuery::output);
            assertOutputDenied(sparkQuery, getDeniedMessage(storage, SPARK_USER, WRITE_ACCESS, CREATE_ACCESS));

            awaitAudit(storage, offset, NIFI_USER, NAMESPACE_RESOURCE_FORMAT.formatted(namespaceName), AuditEvent.DENIED, NO_POLICY);
            awaitAudit(storage, offset, TRINO_USER, tablePath, AuditEvent.DENIED, policyId -> policyId == tableDeny);
            awaitAudit(storage, offset, SPARK_USER, tablePath, AuditEvent.DENIED, policyId -> policyId == tableDeny);
        } finally {
            deletePolicies(policies);
            cluster.grantNifiWrite(storage);
            cluster.awaitPolicyRefresh(storage);
        }

        putRecords(table, List.of(new Row(6, "nifi-6")));
        insertRow(cluster.trino(), TrinoEngine.CATALOG, new Row(7, "trino-7"), table);
        insertRow(cluster.spark(), SparkEngine.CATALOG, new Row(8, "spark-8"), table);
        assertRows(catalog, table);
        assertEngineRows(cluster.trino(), TrinoEngine.CATALOG, table);
    }

    @ParameterizedTest
    @EnumSource(Storage.class)
    @Order(8)
    void testNamespaceOutsideRangerGrantsNotCreated(final Storage storage) throws Exception {
        final String namespaceName = DENIED_NAMESPACE_FORMAT.formatted(getStorageSuffix(storage));
        final Namespace namespace = Namespace.of(namespaceName);
        final String location = NAMESPACE_LOCATION_FORMAT.formatted(IcebergTestCluster.deniedLocation(storage), namespaceName);
        final String deniedResource = IcebergTestCluster.deniedResource(storage, namespaceName);
        final HiveMetastoreCatalog catalog = enableCatalog(ClusterContext.NIFI_KEYTAB).getHiveCatalog();
        final int offset = cluster.auditLines(storage).size();

        try {
            final RuntimeException exception = assertThrows(RuntimeException.class, () -> catalog.createNamespace(namespace, Map.of(LOCATION_PROPERTY, location)));
            assertDenied(exception, List.of(EXTERNAL_PATH_FAILED));
            assertFalse(catalog.namespaceExists(namespace));
            awaitAudit(storage, offset, NIFI_USER, deniedResource, AuditEvent.DENIED, NO_POLICY);
        } finally {
            if (catalog.namespaceExists(namespace)) {
                catalog.dropNamespace(namespace);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Storage.class)
    @Order(9)
    void testRangerPoliciesDecidePerUserAndTable(final Storage storage) throws Exception {
        final String namespaceName = SECURED_NAMESPACE_FORMAT.formatted(getStorageSuffix(storage));
        final Namespace namespace = Namespace.of(namespaceName);
        final String namespaceLocation = NAMESPACE_LOCATION_FORMAT.formatted(IcebergTestCluster.securedLocation(storage), namespaceName);
        final SecuredTables tables = new SecuredTables(storage, namespaceName,
                new TableRows(TableIdentifier.of(namespace, ALPHA_TABLE_NAME), new ArrayList<>()),
                new TableRows(TableIdentifier.of(namespace, BETA_TABLE_NAME), new ArrayList<>()),
                new TableRows(TableIdentifier.of(namespace, GAMMA_TABLE_NAME), new ArrayList<>()));

        final HiveMetastoreCatalog catalog = enableCatalog(ClusterContext.NIFI_KEYTAB).getHiveCatalog();
        catalog.createNamespace(namespace, Map.of(LOCATION_PROPERTY, namespaceLocation));
        setPutIcebergRecord();
        for (final TableRows table : tables.all()) {
            catalog.createTable(table.table(), SCHEMA, PartitionSpec.unpartitioned(), TABLE_LOCATION_FORMAT.formatted(namespaceLocation, table.table().name()), Map.of());
            putRecords(table, List.of(new Row(0, "nifi-0")));
        }
        final String gammaMetadataLocation = ((HasTableOperations) catalog.loadTable(tables.gamma().table())).operations().current().metadataFileLocation();

        final Set<Long> policies = new LinkedHashSet<>();
        try {
            final SecuredPolicies securedPolicies = assertRangerPoliciesDecideAccess(catalog, tables, policies, gammaMetadataLocation);
            assertChangedRangerPoliciesDecideAccess(catalog, tables, securedPolicies, policies, gammaMetadataLocation);
        } finally {
            if (storage == Storage.OZONE) {
                cluster.revoke(storage, IcebergTestCluster.OZONE_SECURED_BUCKET_RESOURCE, SPARK_USER);
                cluster.allow(storage, IcebergTestCluster.OZONE_SECURED_BUCKET_RESOURCE, SPARK_USER, Access.READ);
            }
            deletePolicies(policies);
        }
    }

    @Test
    @Order(10)
    void testKerberosUserServiceRequiredForKerberosConfiguration() throws InitializationException {
        final HiveMetastoreIcebergCatalog service = new HiveMetastoreIcebergCatalog();
        runner.addControllerService(CATALOG_SERVICE_ID, service);
        services.add(service);
        runner.setProperty(service, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, cluster.clientSite().toString());

        runner.assertNotValid(service);
    }

    @Test
    @Order(11)
    void testKeytabOfAnotherPrincipalFailsEnable() throws InitializationException {
        final HiveMetastoreIcebergCatalog service = addCatalog(ClusterContext.HIVE_KEYTAB);

        assertThrows(AssertionError.class, () -> runner.enableControllerService(service));
        assertFalse(runner.isControllerServiceEnabled(service));
    }

    /**
     * Allow, read-only, no-match, recursive and exact-path scope and deny-beats-allow decisions for Spark, Trino and NiFi in
     * one policy refresh, including a Trino namespace creation denied on the storage of the Hive Metastore acting for Trino
     */
    private SecuredPolicies assertRangerPoliciesDecideAccess(final HiveMetastoreCatalog catalog, final SecuredTables tables, final Set<Long> policies,
                                                             final String gammaMetadataLocation) throws Exception {
        final Storage storage = tables.storage();
        final TableRows alpha = tables.alpha();
        final TableRows beta = tables.beta();
        final TableRows gamma = tables.gamma();
        final String gammaMetadataPath = METADATA_RESOURCE_FORMAT.formatted(tables.path(gamma),
                gammaMetadataLocation.substring(gammaMetadataLocation.lastIndexOf(PATH_SEPARATOR) + 1));

        final long alphaPolicy = cluster.allow(storage, tables.resource(alpha), SPARK_USER, Access.WRITE);
        policies.add(alphaPolicy);
        policies.add(cluster.allow(storage, tables.resource(alpha), TRINO_USER, Access.READ));
        final long betaDeny = cluster.deny(storage, tables.resource(beta), NIFI_USER, Access.WRITE);
        policies.add(betaDeny);
        final long gammaDeny = cluster.deny(storage, tables.resource(gamma), NIFI_USER, Access.READ);
        policies.add(gammaDeny);
        final long gammaShallow = cluster.allowWithoutDescendants(storage, gammaMetadataPath, TRINO_USER, Access.READ);
        policies.add(gammaShallow);
        cluster.awaitPolicyRefresh(storage);
        final int offset = cluster.auditLines(storage).size();

        final Row sparkRow = new Row(1, "spark-1");
        final FailedQuery sparkQuery = cluster.spark().queryFailure(String.join(STATEMENT_SEPARATOR,
                insert(SparkEngine.CATALOG, alpha.table(), sparkRow), select(SparkEngine.CATALOG, alpha.table()), select(SparkEngine.CATALOG, beta.table())));
        alpha.rows().add(sparkRow);
        assertEquals(toValues(alpha.rows()), withoutBlankLines(sparkQuery.rows()), sparkQuery::output);
        assertOutputDenied(sparkQuery, getDeniedMessage(storage, SPARK_USER, READ_ACCESS, READ_ACCESS));

        final FailedQuery trinoQuery = cluster.trino().queryFailure(String.join(STATEMENT_SEPARATOR,
                select(TrinoEngine.CATALOG, alpha.table()), insert(TrinoEngine.CATALOG, alpha.table(), new Row(2, "trino-2"))));
        assertEquals(toValues(alpha.rows()), withoutBlankLines(trinoQuery.rows()), trinoQuery::output);
        assertOutputDenied(trinoQuery, getDeniedMessage(storage, TRINO_USER, WRITE_ACCESS, CREATE_ACCESS));
        assertOutputDenied(cluster.trino().queryFailure(select(TrinoEngine.CATALOG, gamma.table())), getDeniedMessage(storage, TRINO_USER, READ_ACCESS, READ_ACCESS));

        final String trinoNamespace = TRINO_DENIED_NAMESPACE_FORMAT.formatted(getStorageSuffix(storage));
        final String trinoLocation = NAMESPACE_LOCATION_FORMAT.formatted(IcebergTestCluster.securedLocation(storage), trinoNamespace);
        cluster.trino().queryFailure(CREATE_SCHEMA_FORMAT.formatted(TrinoEngine.CATALOG, trinoNamespace, trinoLocation));
        assertFalse(cluster.trino().query(SHOW_SCHEMAS_FORMAT.formatted(TrinoEngine.CATALOG)).contains(trinoNamespace));

        assertNiFiDenied(beta.table(), List.of(new Row(1, "denied-1")), getNiFiDeniedMarkers(storage, WRITE_ACCESS, CREATE_ACCESS));
        final RuntimeException gammaException = assertThrows(RuntimeException.class, () -> catalog.loadTable(gamma.table()));
        assertDenied(gammaException, getNiFiDeniedMarkers(storage, READ_ACCESS, READ_ACCESS));
        putRecords(alpha, List.of(new Row(3, "nifi-3")));

        awaitAudit(storage, offset, SPARK_USER, tables.path(alpha), AuditEvent.ALLOWED, policyId -> policyId == alphaPolicy);
        awaitAudit(storage, offset, SPARK_USER, tables.path(beta), AuditEvent.DENIED, NO_POLICY);
        awaitAudit(storage, offset, TRINO_USER, tables.path(alpha), AuditEvent.DENIED, NO_POLICY);
        awaitAudit(storage, offset, TRINO_USER, gammaMetadataPath, AuditEvent.ALLOWED, policyId -> policyId == gammaShallow);
        awaitAudit(storage, offset, TRINO_USER, tables.path(gamma), AuditEvent.DENIED, NO_POLICY);
        awaitAudit(storage, offset, TRINO_USER, NAMESPACE_RESOURCE_FORMAT.formatted(trinoNamespace), AuditEvent.DENIED, NO_POLICY);
        awaitAudit(storage, offset, NIFI_USER, tables.path(beta), AuditEvent.DENIED, policyId -> policyId == betaDeny);
        awaitAudit(storage, offset, NIFI_USER, tables.path(gamma), AuditEvent.DENIED, policyId -> policyId == gammaDeny);
        return new SecuredPolicies(alphaPolicy, betaDeny, gammaShallow);
    }

    /**
     * Revoked, updated, disabled and replaced policies change the decisions of the first phase in one policy refresh,
     * while users, files and POSIX permissions stay unchanged
     */
    private void assertChangedRangerPoliciesDecideAccess(final HiveMetastoreCatalog catalog, final SecuredTables tables, final SecuredPolicies securedPolicies,
                                                         final Set<Long> policies, final String gammaMetadataLocation) throws Exception {
        final Storage storage = tables.storage();
        final TableRows alpha = tables.alpha();
        final TableRows beta = tables.beta();
        final TableRows gamma = tables.gamma();
        final String alphaDataPath = DATA_RESOURCE_FORMAT.formatted(tables.path(alpha));

        final String sparkRevokedResource = storage == Storage.HDFS ? tables.resource(alpha) : IcebergTestCluster.OZONE_SECURED_BUCKET_RESOURCE;
        cluster.revoke(storage, sparkRevokedResource, SPARK_USER);
        cluster.allow(storage, tables.resource(alpha), TRINO_USER, Access.WRITE);
        final long alphaDataDeny = cluster.deny(storage, IcebergTestCluster.securedDataResource(storage, tables.path(alpha)), TRINO_USER, Access.READ);
        policies.add(alphaDataDeny);
        cluster.setPolicyEnabled(securedPolicies.betaDeny(), false);
        cluster.revoke(storage, tables.resource(gamma), NIFI_USER);
        cluster.deletePolicy(securedPolicies.gammaShallow());
        policies.add(cluster.allow(storage, tables.resource(gamma), TRINO_USER, Access.READ));
        cluster.awaitPolicyRefresh(storage);
        final int offset = cluster.auditLines(storage).size();

        insertRow(cluster.trino(), TrinoEngine.CATALOG, new Row(4, "trino-4"), alpha);
        final long snapshots = StreamSupport.stream(catalog.loadTable(alpha.table()).snapshots().spliterator(), false).count();
        final FailedQuery trinoQuery = cluster.trino().queryFailure(String.join(STATEMENT_SEPARATOR,
                snapshotsCount(TrinoEngine.CATALOG, alpha.table()), select(TrinoEngine.CATALOG, alpha.table())));
        assertEquals(List.of(Long.toString(snapshots)), withoutBlankLines(trinoQuery.rows()), trinoQuery::output);
        assertOutputDenied(trinoQuery, getDeniedMessage(storage, TRINO_USER, READ_ACCESS, READ_ACCESS));

        final String sparkDenied = switch (storage) {
            case HDFS -> getDeniedMessage(storage, SPARK_USER, READ_ACCESS, READ_ACCESS);
            case OZONE -> OZONE_DENIED_RESOURCE_FORMAT.formatted(SPARK_USER, READ_ACCESS, BUCKET_RESOURCE_TYPE);
        };
        assertOutputDenied(cluster.spark().queryFailure(select(SparkEngine.CATALOG, alpha.table())), sparkDenied);

        putRecords(beta, List.of(new Row(2, "nifi-2")));
        for (final TableRows table : tables.all()) {
            assertRows(catalog, table);
        }
        assertEngineRows(cluster.trino(), TrinoEngine.CATALOG, gamma);

        final FailedQuery dropQuery = cluster.trino().queryFailure(DROP_TABLE_FORMAT.formatted(TrinoEngine.CATALOG, tables.namespaceName(), gamma.table().name()));
        final String dropDenied = switch (storage) {
            case HDFS -> getDeniedMessage(storage, TRINO_USER, WRITE_ACCESS, DELETE_ACCESS);
            case OZONE -> DELETE_DIRECTORY_FAILED;
        };
        assertOutputDenied(dropQuery, dropDenied);
        assertFalse(cluster.trino().query(SHOW_TABLES_FORMAT.formatted(TrinoEngine.CATALOG, tables.namespaceName())).contains(gamma.table().name()),
                "Hive Metastore without authorization drops the table entry although Ranger denied deleting the table files");
        catalog.registerTable(gamma.table(), gammaMetadataLocation);
        assertRows(catalog, gamma);

        if (storage == Storage.HDFS) {
            awaitAudit(storage, offset, SPARK_USER, tables.path(alpha), AuditEvent.DENIED, NO_POLICY);
        } else {
            final String bucketResource = IcebergTestCluster.OZONE_SECURED_BUCKET_RESOURCE;
            awaitAudit(storage, offset, AUDIT_DESCRIPTION_FORMAT.formatted(SPARK_USER, bucketResource, AuditEvent.DENIED),
                    isEvent(SPARK_USER, bucketResource, AuditEvent.DENIED, NO_POLICY).and(event -> bucketResource.equals(event.resource())));
        }
        awaitAudit(storage, offset, TRINO_USER, tables.path(alpha), AuditEvent.ALLOWED, policyId -> policyId == securedPolicies.alpha());
        awaitAudit(storage, offset, TRINO_USER, alphaDataPath, AuditEvent.DENIED, policyId -> policyId == alphaDataDeny);
        awaitAudit(storage, offset, NIFI_USER, tables.path(beta), AuditEvent.ALLOWED, policyId -> policyId != AuditEvent.NO_POLICY && policyId != securedPolicies.betaDeny());
        awaitAudit(storage, offset, NIFI_USER, tables.path(gamma), AuditEvent.ALLOWED, ANY_POLICY);
        awaitAudit(storage, offset, AUDIT_DESCRIPTION_FORMAT.formatted(TRINO_USER, tables.path(gamma), AuditEvent.DENIED),
                isEvent(TRINO_USER, tables.path(gamma), AuditEvent.DENIED, NO_POLICY).and(event -> storage == Storage.HDFS || DELETE_AUDIT_ACCESS.equals(event.access())));
    }

    private void assertNiFiDenied(final TableIdentifier table, final List<Row> rows, final List<String> markers) throws InitializationException {
        runPutIcebergRecord(table, rows);
        assertEquals(1, runner.getFlowFilesForRelationship(FAILURE_RELATIONSHIP).size(), () -> runner.getLogger().getErrorMessages().toString());
        final List<LogMessage> errors = runner.getLogger().getErrorMessages();
        assertTrue(errors.stream().anyMatch(message -> containsMarkers(message.getThrowable(), markers)), errors::toString);
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

    private void putRecords(final TableRows table, final List<Row> rows) throws InitializationException {
        runPutIcebergRecord(table.table(), rows);
        assertEquals(1, runner.getFlowFilesForRelationship(SUCCESS_RELATIONSHIP).size(), () -> runner.getLogger().getErrorMessages().toString());
        table.rows().addAll(rows);
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

    private static void assertRows(final HiveMetastoreCatalog catalog, final TableRows table) throws IOException {
        final List<Row> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(catalog.loadTable(table.table())).build()) {
            records.forEach(record -> rows.add(new Row((Long) record.getField(IDENTIFIER_FIELD), (String) record.getField(DESCRIPTION_FIELD))));
        }
        rows.sort(Comparator.comparingLong(Row::id));

        assertEquals(table.rows().stream().sorted(Comparator.comparingLong(Row::id)).toList(), rows, table.table()::toString);
    }

    private static void assertEngineRows(final QueryEngine engine, final String catalog, final TableRows... tables) throws Exception {
        final String query = Arrays.stream(tables).map(table -> select(catalog, table.table())).collect(Collectors.joining(STATEMENT_SEPARATOR));
        final List<String> expected = Arrays.stream(tables).flatMap(table -> toValues(table.rows()).stream()).toList();

        assertEquals(expected, withoutBlankLines(engine.query(query)), () -> "%s %s".formatted(engine.getClass().getSimpleName(), query));
    }

    private static void insertRow(final QueryEngine engine, final String catalog, final Row row, final TableRows... tables) throws Exception {
        engine.query(Arrays.stream(tables).map(table -> insert(catalog, table.table(), row)).collect(Collectors.joining(STATEMENT_SEPARATOR)));
        for (final TableRows table : tables) {
            table.rows().add(row);
        }
    }

    private static void assertOutputDenied(final FailedQuery failedQuery, final String deniedMessage) {
        assertTrue(failedQuery.output().contains(deniedMessage), () -> "[%s] not found in output:%n%s".formatted(deniedMessage, failedQuery.output()));
    }

    private static void assertDenied(final Throwable throwable, final List<String> markers) {
        assertTrue(containsMarkers(throwable, markers), () -> "%s not found in causes of %s".formatted(markers, throwable));
    }

    private static void awaitAudit(final Storage storage, final int offset, final String user, final String resource, final int result, final LongPredicate policy)
            throws InterruptedException {
        awaitAudit(storage, offset, AUDIT_DESCRIPTION_FORMAT.formatted(user, resource, result), isEvent(user, resource, result, policy));
    }

    private static void awaitAudit(final Storage storage, final int offset, final String description, final Predicate<AuditEvent> predicate) throws InterruptedException {
        ClusterFiles.waitUntil("Ranger %s audit %s".formatted(storage, description), AUDIT_TIMEOUT, () -> {
            final List<AuditEvent> events = getAuditEvents(storage, offset);
            final AuditEvent event = events.stream()
                    .filter(predicate)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Audit event not found in %d events".formatted(events.size())));
            assertNotEquals(FALLBACK_ENFORCER, event.enforcer(), event::toString);
        });
    }

    private static Predicate<AuditEvent> isEvent(final String user, final String resource, final int result, final LongPredicate policy) {
        return event -> user.equals(event.user()) && event.resource().contains(resource) && event.result() == result && policy.test(event.policyId());
    }

    private static List<AuditEvent> getAuditEvents(final Storage storage, final int offset) throws Exception {
        final List<String> lines = cluster.auditLines(storage);
        return lines.subList(Math.min(offset, lines.size()), lines.size()).stream()
                .map(AuditEvent::parse)
                .flatMap(Optional::stream)
                .toList();
    }

    private static void deletePolicies(final Set<Long> policies) throws Exception {
        for (final long policyId : policies) {
            cluster.deletePolicy(policyId);
        }
    }

    private static String getStorageSuffix(final Storage storage) {
        return storage.name().toLowerCase(Locale.ROOT);
    }

    private static TableRows getRows(final Storage storage) {
        return switch (storage) {
            case HDFS -> HDFS_ROWS;
            case OZONE -> OZONE_ROWS;
        };
    }

    private static String getDeniedMessage(final Storage storage, final String user, final String hdfsAccess, final String ozoneAclType) {
        return switch (storage) {
            case HDFS -> HDFS_DENIED_FORMAT.formatted(user, hdfsAccess);
            case OZONE -> OZONE_DENIED_FORMAT.formatted(user, ozoneAclType);
        };
    }

    private static List<String> getNiFiDeniedMarkers(final Storage storage, final String hdfsAccess, final String ozoneAclType) {
        final String deniedMessage = getDeniedMessage(storage, NIFI_USER, hdfsAccess, ozoneAclType);
        return switch (storage) {
            case HDFS -> List.of(deniedMessage, HDFS_DENIED_EXCEPTION);
            case OZONE -> List.of(deniedMessage);
        };
    }

    private static String select(final String catalog, final TableIdentifier table) {
        return SELECT_FORMAT.formatted(IDENTIFIER_FIELD, DESCRIPTION_FIELD, catalog, table.namespace().level(0), table.name(), IDENTIFIER_FIELD);
    }

    private static String createTable(final String format, final String catalog, final TableIdentifier table) {
        return format.formatted(catalog, table.namespace().level(0), table.name(), IDENTIFIER_FIELD, DESCRIPTION_FIELD);
    }

    private static String insert(final String catalog, final TableIdentifier table, final Row row) {
        return INSERT_FORMAT.formatted(catalog, table.namespace().level(0), table.name(), row.id(), row.description());
    }

    private static String snapshotsCount(final String catalog, final TableIdentifier table) {
        return SNAPSHOTS_COUNT_FORMAT.formatted(catalog, table.namespace().level(0), table.name());
    }

    private static List<String> toValues(final List<Row> rows) {
        return rows.stream().sorted(Comparator.comparingLong(Row::id)).map(Row::toValues).toList();
    }

    private static List<String> withoutBlankLines(final List<String> lines) {
        return lines.stream().filter(line -> !line.isBlank()).toList();
    }

    private static String getOwner(final HiveMetastoreCatalog catalog, final Namespace namespace) throws Exception {
        return catalog.getClientPool().run(client -> client.getDatabase(namespace.level(0)).getOwnerName());
    }

    private static boolean containsMarkers(final Throwable throwable, final List<String> markers) {
        Throwable cause = throwable;
        while (cause != null) {
            final String description = cause.toString();
            if (markers.stream().allMatch(description::contains)) {
                return true;
            }
            cause = cause.getCause() == cause ? null : cause.getCause();
        }
        return false;
    }

    private record Row(long id, String description) {
        private String toValues() {
            return "%d\t%s".formatted(id, description);
        }
    }

    private record TableRows(TableIdentifier table, List<Row> rows) {
    }

    private record SecuredTables(Storage storage, String namespaceName, TableRows alpha, TableRows beta, TableRows gamma) {
        private List<TableRows> all() {
            return List.of(alpha, beta, gamma);
        }

        private String path(final TableRows table) {
            return TABLE_RESOURCE_FORMAT.formatted(namespaceName, table.table().name());
        }

        private String resource(final TableRows table) {
            return IcebergTestCluster.securedResource(storage, path(table));
        }
    }

    private record SecuredPolicies(long alpha, long betaDeny, long gammaShallow) {
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
