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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the Catalog paths that complete without a Hive Metastore connection: configuration, identifier validation
 * and resource cleanup.
 */
class HiveMetastoreCatalogTest {
    private static final String CATALOG_NAME = "hive-metastore-catalog";

    private static final String METASTORE_URI = "thrift://127.0.0.1:1";

    private static final String WAREHOUSE_LOCATION = "file:/tmp/warehouse";

    private static final String CLIENT_POOL_SIZE = "3";

    private static final String CONFIGURATION_PROPERTY = "nifi.catalog.test";

    private static final String CONFIGURATION_VALUE = "ADS-3777";

    private static final String TABLE_NAME = "records";

    private static final Namespace NAMESPACE = Namespace.of("nifi");

    private static final Namespace NESTED_NAMESPACE = Namespace.of("nifi", "nested");

    private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, TABLE_NAME);

    private static final TableIdentifier NESTED_TABLE_IDENTIFIER = TableIdentifier.of(NESTED_NAMESPACE, TABLE_NAME);

    private HiveMetastoreCatalog catalog;

    @BeforeEach
    void setCatalog() {
        catalog = new HiveMetastoreCatalog();
    }

    @AfterEach
    void closeCatalog() throws IOException {
        catalog.close();
    }

    @Test
    void testInitializeSetsMetastoreProperties() {
        catalog.initialize(CATALOG_NAME, Map.of(
                CatalogProperties.URI, METASTORE_URI,
                CatalogProperties.WAREHOUSE_LOCATION, "%s/".formatted(WAREHOUSE_LOCATION)
        ));

        assertEquals(CATALOG_NAME, catalog.name());
        assertEquals(METASTORE_URI, catalog.getConf().get(HiveMetastoreCatalog.METASTORE_URIS_PROPERTY));
        assertEquals(WAREHOUSE_LOCATION, catalog.getConf().get(HiveMetastoreCatalog.METASTORE_WAREHOUSE_PROPERTY));
        assertTrue(catalog.toString().contains(METASTORE_URI), catalog.toString());
    }

    @Test
    void testInitializeWithoutPropertiesKeepsConfiguration() {
        final Configuration configuration = new Configuration(false);
        configuration.set(HiveMetastoreCatalog.METASTORE_URIS_PROPERTY, METASTORE_URI);
        catalog.setConf(configuration);

        catalog.initialize(CATALOG_NAME, Map.of());

        assertEquals(METASTORE_URI, catalog.getConf().get(HiveMetastoreCatalog.METASTORE_URIS_PROPERTY));
        assertNull(catalog.getConf().get(HiveMetastoreCatalog.METASTORE_WAREHOUSE_PROPERTY));
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
    void testFileIODefaultHadoopFileIO() {
        catalog.initialize(CATALOG_NAME, Map.of(CatalogProperties.URI, METASTORE_URI));

        assertInstanceOf(HadoopFileIO.class, catalog.getFileIO());
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

        assertTrue(exception.getMessage().contains(HiveMetastoreCatalog.METASTORE_WAREHOUSE_PROPERTY), exception.getMessage());
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
