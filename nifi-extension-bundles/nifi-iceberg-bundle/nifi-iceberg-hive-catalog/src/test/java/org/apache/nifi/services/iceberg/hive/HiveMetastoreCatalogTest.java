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
import org.apache.hadoop.hdfs.ClientContext;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.security.krb.KerberosUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Covers the Catalog paths that complete without a Hive Metastore connection: configuration, identifier validation
 * and resource cleanup.
 */
class HiveMetastoreCatalogTest {
    private static final String CATALOG_NAME = "hive-metastore-catalog";

    private static final String METASTORE_URI = "thrift://127.0.0.1:1";

    private static final String OTHER_METASTORE_URI = "thrift://127.0.0.1:2";

    private static final String WAREHOUSE_LOCATION = "file:/tmp/warehouse";

    private static final String METASTORE_URIS_PROPERTY = MetastoreConf.ConfVars.THRIFT_URIS.getHiveName();

    private static final String METASTORE_THRIFT_URIS_PROPERTY = MetastoreConf.ConfVars.THRIFT_URIS.getVarname();

    private static final String METASTORE_WAREHOUSE_PROPERTY = MetastoreConf.ConfVars.WAREHOUSE.getHiveName();

    private static final String METASTORE_WAREHOUSE_DIR_PROPERTY = MetastoreConf.ConfVars.WAREHOUSE.getVarname();

    private static final String CLIENT_POOL_SIZE = "3";

    private static final String CONFIGURATION_PROPERTY = "nifi.catalog.test";

    private static final String CONFIGURATION_VALUE = "ADS-3777";

    private static final String TABLE_NAME = "records";

    private static final String FIRST_USER = "nifi-first";

    private static final String SECOND_USER = "nifi-second";

    private static final String CONFIGURED_CLIENT_CONTEXT = "configured";

    private static final String MISSING_METRICS_REPORTER_CLASS = "org.example.MissingMetricsReporter";

    private static final Namespace NAMESPACE = Namespace.of("nifi");

    private static final Namespace NESTED_NAMESPACE = Namespace.of("nifi", "nested");

    private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, TABLE_NAME);

    private static final TableIdentifier NESTED_TABLE_IDENTIFIER = TableIdentifier.of(NESTED_NAMESPACE, TABLE_NAME);

    private final List<Closeable> closeables = new ArrayList<>();

    private HadoopIdentity identity;

    private HiveMetastoreCatalog catalog;

    @BeforeEach
    void setCatalog() throws IOException {
        identity = HadoopIdentity.login(new Configuration(false), null, mock(ComponentLog.class));
        catalog = new HiveMetastoreCatalog(identity);
    }

    @AfterEach
    void closeCatalog() throws IOException {
        catalog.close();
        identity.close();
        for (final Closeable closeable : closeables) {
            closeable.close();
        }
    }

    @Test
    void testInitializeSetsMetastoreProperties() {
        catalog.initialize(CATALOG_NAME, Map.of(
                CatalogProperties.URI, METASTORE_URI,
                CatalogProperties.WAREHOUSE_LOCATION, "%s/".formatted(WAREHOUSE_LOCATION)
        ));

        assertEquals(CATALOG_NAME, catalog.name());
        assertEquals(METASTORE_URI, catalog.getConf().get(METASTORE_URIS_PROPERTY));
        assertEquals(WAREHOUSE_LOCATION, catalog.getConf().get(METASTORE_WAREHOUSE_PROPERTY));
        assertEquals(METASTORE_URI, catalog.getConf().get(METASTORE_THRIFT_URIS_PROPERTY));
        assertEquals(WAREHOUSE_LOCATION, catalog.getConf().get(METASTORE_WAREHOUSE_DIR_PROPERTY));
        assertTrue(catalog.toString().contains(METASTORE_URI), catalog.toString());
    }

    @Test
    void testUriOverridesMetastoreThriftUrisFromConfiguration() {
        final Configuration configuration = new Configuration(false);
        configuration.set(METASTORE_THRIFT_URIS_PROPERTY, OTHER_METASTORE_URI);
        catalog.setConf(configuration);

        catalog.initialize(CATALOG_NAME, Map.of(CatalogProperties.URI, METASTORE_URI));

        assertEquals(METASTORE_URI, MetastoreConf.getVar(catalog.getConf(), MetastoreConf.ConfVars.THRIFT_URIS));
        assertTrue(catalog.toString().contains(METASTORE_URI), catalog.toString());
    }

    @Test
    void testCreateNamespaceWithMetastoreWarehouseDirFromConfiguration() {
        final Configuration configuration = new Configuration(false);
        configuration.set(METASTORE_WAREHOUSE_DIR_PROPERTY, WAREHOUSE_LOCATION);
        MetastoreConf.setLongVar(configuration, MetastoreConf.ConfVars.THRIFT_CONNECTION_RETRIES, 1);
        MetastoreConf.setLongVar(configuration, MetastoreConf.ConfVars.THRIFT_FAILURE_RETRIES, 0);
        MetastoreConf.setTimeVar(configuration, MetastoreConf.ConfVars.CLIENT_CONNECT_RETRY_DELAY, 0, TimeUnit.SECONDS);
        catalog.setConf(configuration);
        catalog.initialize(CATALOG_NAME, Map.of(CatalogProperties.URI, METASTORE_URI));

        final RuntimeException exception = assertThrows(RuntimeException.class, () -> catalog.createNamespace(NAMESPACE));

        assertFalse(exception instanceof IllegalStateException, exception::toString);
    }

    @Test
    void testInitializeWithoutPropertiesKeepsConfiguration() {
        final Configuration configuration = new Configuration(false);
        configuration.set(METASTORE_URIS_PROPERTY, METASTORE_URI);
        catalog.setConf(configuration);

        catalog.initialize(CATALOG_NAME, Map.of());

        assertEquals(METASTORE_URI, catalog.getConf().get(METASTORE_URIS_PROPERTY));
        assertNull(catalog.getConf().get(METASTORE_WAREHOUSE_PROPERTY));
    }

    @Test
    void testInitializeFailsWithInvalidMetricsReporter() {
        final Map<String, String> properties = Map.of(CatalogProperties.URI, METASTORE_URI, CatalogProperties.METRICS_REPORTER_IMPL, MISSING_METRICS_REPORTER_CLASS);

        assertThrows(IllegalArgumentException.class, () -> catalog.initialize(CATALOG_NAME, properties));
    }

    @Test
    void testSetConfCopiesConfiguration() {
        final Configuration configuration = new Configuration(false);
        catalog.setConf(configuration);
        configuration.set(CONFIGURATION_PROPERTY, CONFIGURATION_VALUE);

        assertNull(catalog.getConf().get(CONFIGURATION_PROPERTY));
    }

    @Test
    void testClientPoolSizeFromProperties() {
        catalog.initialize(CATALOG_NAME, Map.of(CatalogProperties.URI, METASTORE_URI, CatalogProperties.CLIENT_POOL_SIZE, CLIENT_POOL_SIZE));

        assertEquals(Integer.parseInt(CLIENT_POOL_SIZE), catalog.getClientPool().poolSize());
    }

    @Test
    void testFileIODefaultPrivilegedHadoopFileIO() {
        catalog.initialize(CATALOG_NAME, Map.of(CatalogProperties.URI, METASTORE_URI));

        final PrivilegedFileIO fileIO = assertInstanceOf(PrivilegedFileIO.class, catalog.getFileIO());
        assertInstanceOf(HadoopFileIO.class, fileIO.getDelegate());
    }

    @Test
    void testClientContextPerPrincipal() throws Exception {
        final HiveMetastoreCatalog first = getCatalog(FIRST_USER);
        final HiveMetastoreCatalog second = getCatalog(SECOND_USER);

        final String firstContext = first.getConf().get(HiveMetastoreCatalog.CLIENT_CONTEXT_PROPERTY);
        final String secondContext = second.getConf().get(HiveMetastoreCatalog.CLIENT_CONTEXT_PROPERTY);

        assertTrue(firstContext.endsWith(FIRST_USER), firstContext);
        assertTrue(secondContext.endsWith(SECOND_USER), secondContext);
        assertNotSame(ClientContext.get(firstContext, first.getConf()).getKeyProviderCache(), ClientContext.get(secondContext, second.getConf()).getKeyProviderCache());
    }

    @Test
    void testDeadNodeDetectionDisabled() {
        final Configuration configuration = new Configuration(false);
        configuration.setBoolean(HiveMetastoreCatalog.DEAD_NODE_DETECTION_PROPERTY, true);
        catalog.setConf(configuration);

        catalog.initialize(CATALOG_NAME, Map.of(CatalogProperties.URI, METASTORE_URI));

        assertFalse(catalog.getConf().getBoolean(HiveMetastoreCatalog.DEAD_NODE_DETECTION_PROPERTY, true));
    }

    @Test
    void testClientContextFromConfigurationPreserved() {
        final Configuration configuration = new Configuration(false);
        configuration.set(HiveMetastoreCatalog.CLIENT_CONTEXT_PROPERTY, CONFIGURED_CLIENT_CONTEXT);
        catalog.setConf(configuration);

        catalog.initialize(CATALOG_NAME, Map.of(CatalogProperties.URI, METASTORE_URI));

        assertEquals(CONFIGURED_CLIENT_CONTEXT, catalog.getConf().get(HiveMetastoreCatalog.CLIENT_CONTEXT_PROPERTY));
    }

    private HiveMetastoreCatalog getCatalog(final String userName) throws Exception {
        final KerberosUser kerberosUser = mock(KerberosUser.class);
        doReturn(UserGroupInformation.createRemoteUser(userName)).when(kerberosUser).doAs(any(PrivilegedExceptionAction.class));
        final HadoopIdentity userIdentity = HadoopIdentity.login(new Configuration(false), kerberosUser, mock(ComponentLog.class));
        closeables.add(userIdentity);

        final HiveMetastoreCatalog userCatalog = new HiveMetastoreCatalog(userIdentity);
        userCatalog.setConf(new Configuration(false));
        userCatalog.initialize(CATALOG_NAME, Map.of(CatalogProperties.URI, METASTORE_URI));
        closeables.add(userCatalog);
        return userCatalog;
    }

    @Test
    void testFileIOFromProperties() {
        catalog.initialize(CATALOG_NAME, Map.of(CatalogProperties.URI, METASTORE_URI, CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName()));

        assertInstanceOf(InMemoryFileIO.class, catalog.getFileIO());
    }

    @Test
    void testCloseClosesClientPool() throws IOException {
        catalog.initialize(CATALOG_NAME, Map.of(CatalogProperties.URI, METASTORE_URI));

        catalog.close();

        assertTrue(catalog.getClientPool().isClosed());
    }

    @Test
    void testCloseWithoutInitialize() {
        assertDoesNotThrow(catalog::close);
    }

    @Test
    void testCreateNamespaceNestedNotSupported() {
        catalog.initialize(CATALOG_NAME, Map.of(CatalogProperties.URI, METASTORE_URI, CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE_LOCATION));

        assertThrows(NoSuchNamespaceException.class, () -> catalog.createNamespace(NESTED_NAMESPACE));
    }

    @Test
    void testCreateNamespaceWithoutWarehouseLocation() {
        catalog.initialize(CATALOG_NAME, Map.of(CatalogProperties.URI, METASTORE_URI));

        final IllegalStateException exception = assertThrows(IllegalStateException.class, () -> catalog.createNamespace(NAMESPACE));

        assertTrue(exception.getMessage().contains(METASTORE_WAREHOUSE_DIR_PROPERTY), exception.getMessage());
    }

    @Test
    void testNestedNamespaceOperations() {
        catalog.initialize(CATALOG_NAME, Map.of(CatalogProperties.URI, METASTORE_URI));

        assertFalse(catalog.dropNamespace(NESTED_NAMESPACE));
        assertThrows(NoSuchNamespaceException.class, () -> catalog.loadNamespaceMetadata(NESTED_NAMESPACE));
        assertThrows(NoSuchNamespaceException.class, () -> catalog.listNamespaces(NESTED_NAMESPACE));
        assertThrows(NoSuchNamespaceException.class, () -> catalog.listTables(NESTED_NAMESPACE));
    }

    @Test
    void testNestedTableIdentifierOperations() {
        catalog.initialize(CATALOG_NAME, Map.of(CatalogProperties.URI, METASTORE_URI));

        assertFalse(catalog.dropTable(NESTED_TABLE_IDENTIFIER, true));
        assertThrows(NoSuchTableException.class, () -> catalog.loadTable(NESTED_TABLE_IDENTIFIER));
        assertThrows(NoSuchTableException.class, () -> catalog.renameTable(TABLE_IDENTIFIER, NESTED_TABLE_IDENTIFIER));
        assertThrows(NoSuchTableException.class, () -> catalog.renameTable(NESTED_TABLE_IDENTIFIER, TABLE_IDENTIFIER));
    }
}
