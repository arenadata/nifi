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
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HiveMetastoreIcebergCatalogTest {
    private static final String SERVICE_ID = HiveMetastoreIcebergCatalog.class.getSimpleName();

    private static final String METASTORE_URI = "thrift://metastore.example.com:9083";

    private static final String WAREHOUSE_LOCATION = "hdfs://cluster/warehouse";

    private static final String METASTORE_URIS_PROPERTY = "hive.metastore.uris";

    private static final String WAREHOUSE_DIRECTORY_PROPERTY = "hive.metastore.warehouse.dir";

    private static final String METASTORE_THRIFT_URIS_PROPERTY = "metastore.thrift.uris";

    private static final String OTHER_METASTORE_URI = "thrift://other.example.com:9083";

    private static final String KERBEROS_SERVICE_ID = "kerberos-user-service";

    private static final String PRINCIPAL = "nifi@EXAMPLE.COM";

    private static final String DYNAMIC_PROPERTY_NAME = "clients";

    private static final String DYNAMIC_PROPERTY_VALUE = "4";

    private static final String CLIENT_POOL_CACHE_KEYS_PROPERTY = "client-pool-cache-keys";

    private static final String CLIENT_POOL_CACHE_KEY_PROPERTY = "nifi.iceberg.catalog.client-pool-key";

    private static final String OZONE_FILE_SYSTEM_PROPERTY = "fs.ofs.impl";

    private static final String OZONE_FILE_SYSTEM_CLASS = "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem";

    private static final String CUSTOM_FILE_SYSTEM_CLASS = "org.example.CustomFileSystem";

    private static final String FILE_IO_PROPERTY = "io-impl";

    private static final String MISSING_FILE_IO_CLASS = "org.example.MissingFileIO";

    private static final String CONFIGURATION_FORMAT = """
            <?xml version="1.0" encoding="UTF-8"?>
            <configuration>
                <property>
                    <name>%s</name>
                    <value>%s</value>
                </property>
            </configuration>""";

    @TempDir
    private Path tempDirectory;

    private TestRunner runner;

    private HiveMetastoreIcebergCatalog catalogService;

    @BeforeEach
    void setCatalogService() throws InitializationException {
        catalogService = new HiveMetastoreIcebergCatalog();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService(SERVICE_ID, catalogService);
    }

    @AfterEach
    void disableCatalogService() {
        if (runner.isControllerServiceEnabled(catalogService)) {
            runner.disableControllerService(catalogService);
        }
    }

    @Test
    void testNotValidWithoutMetastoreUri() {
        runner.assertNotValid(catalogService);
    }

    @Test
    void testValidWithMetastoreUriProperty() {
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, METASTORE_URI);

        runner.assertValid(catalogService);
    }

    @Test
    void testValidWithMetastoreUriFromConfigurationResources() throws IOException {
        final Path configuration = writeConfiguration(METASTORE_URIS_PROPERTY, METASTORE_URI);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, configuration.toString());

        runner.assertValid(catalogService);
    }

    @Test
    void testNotValidWithConfigurationResourcesWithoutMetastoreUri() throws IOException {
        final Path configuration = writeConfiguration(WAREHOUSE_DIRECTORY_PROPERTY, WAREHOUSE_LOCATION);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, configuration.toString());

        runner.assertNotValid(catalogService);
    }

    @Test
    void testGetCatalog() {
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, METASTORE_URI);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.WAREHOUSE_LOCATION, WAREHOUSE_LOCATION);
        runner.enableControllerService(catalogService);

        final HiveCatalog hiveCatalog = catalogService.getHiveCatalog();
        assertEquals(SERVICE_ID, hiveCatalog.name());

        final Configuration configuration = hiveCatalog.getConf();
        assertNotNull(configuration);
        assertEquals(METASTORE_URI, configuration.get(METASTORE_URIS_PROPERTY));
        assertEquals(WAREHOUSE_LOCATION, configuration.get(WAREHOUSE_DIRECTORY_PROPERTY));
    }

    @Test
    void testGetCatalogWithConfigurationResources() throws IOException {
        final Path configuration = writeConfiguration(METASTORE_URIS_PROPERTY, METASTORE_URI);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, configuration.toString());
        runner.enableControllerService(catalogService);

        final HiveCatalog hiveCatalog = catalogService.getHiveCatalog();
        assertEquals(METASTORE_URI, hiveCatalog.getConf().get(METASTORE_URIS_PROPERTY));
    }

    @Test
    void testValidWithMetastoreThriftUrisFromConfigurationResources() throws IOException {
        final Path configuration = writeConfiguration(METASTORE_THRIFT_URIS_PROPERTY, METASTORE_URI);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, configuration.toString());

        runner.assertValid(catalogService);
    }

    @Test
    void testGetCatalogMetastoreUriOverridesConfigurationResources() throws IOException {
        final Path configuration = writeConfiguration(METASTORE_THRIFT_URIS_PROPERTY, OTHER_METASTORE_URI);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, configuration.toString());
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, METASTORE_URI);
        runner.enableControllerService(catalogService);

        final HiveCatalog hiveCatalog = catalogService.getHiveCatalog();
        final Configuration hiveConfiguration = hiveCatalog.getConf();

        assertEquals(METASTORE_URI, hiveConfiguration.get(METASTORE_URIS_PROPERTY));
        assertEquals(METASTORE_URI, hiveConfiguration.get(METASTORE_THRIFT_URIS_PROPERTY));
    }

    @Test
    void testGetCatalogDisabled() {
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, METASTORE_URI);
        runner.enableControllerService(catalogService);
        runner.disableControllerService(catalogService);

        assertNull(catalogService.getCatalog());
    }

    @Test
    void testGetClassloaderIsolationKeyWithoutKerberosUserService() {
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, METASTORE_URI);

        assertNull(catalogService.getClassloaderIsolationKey(runner.getProcessContext()));
    }

    @Test
    void testSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = catalogService.getPropertyDescriptors();

        assertEquals(
                List.of(
                        HiveMetastoreIcebergCatalog.METASTORE_URI,
                        HiveMetastoreIcebergCatalog.WAREHOUSE_LOCATION,
                        HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES,
                        HiveMetastoreIcebergCatalog.ADDITIONAL_CLASSPATH_RESOURCES,
                        HiveMetastoreIcebergCatalog.KERBEROS_USER_SERVICE
                ),
                descriptors
        );
    }

    @Test
    void testDynamicPropertySupported() {
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, METASTORE_URI);
        runner.setProperty(catalogService, DYNAMIC_PROPERTY_NAME, DYNAMIC_PROPERTY_VALUE);

        runner.assertValid(catalogService);

        final PropertyDescriptor descriptor = catalogService.getPropertyDescriptor(DYNAMIC_PROPERTY_NAME);
        assertNotNull(descriptor);
        assertTrue(descriptor.isDynamic());
    }

    @Test
    void testMetastoreUriTrailingSeparatorNormalized() {
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, "%s,".formatted(METASTORE_URI));
        runner.enableControllerService(catalogService);

        final HiveCatalog hiveCatalog = catalogService.getHiveCatalog();

        assertEquals(METASTORE_URI, hiveCatalog.getConf().get(METASTORE_URIS_PROPERTY));
    }

    @Test
    void testNotValidWhenMetastoreUriContainsOnlySeparators() {
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, " , ");

        runner.assertNotValid(catalogService);
    }

    @Test
    void testGetClassloaderIsolationKeyWithKerberosUserService() throws InitializationException {
        final KerberosUser kerberosUser = mock(KerberosUser.class);
        when(kerberosUser.getPrincipal()).thenReturn(PRINCIPAL);
        final ConfigurationContext context = getContextWithKerberosUserService(kerberosUser);

        assertEquals(PRINCIPAL, catalogService.getClassloaderIsolationKey(context));
    }

    @Test
    void testKerberosUserLoggedOutOnDisabled() throws InitializationException {
        final KerberosUser kerberosUser = mock(KerberosUser.class);
        when(kerberosUser.getPrincipal()).thenReturn(PRINCIPAL);
        runner.addControllerService(KERBEROS_SERVICE_ID, new MockKerberosUserService(kerberosUser));
        runner.enableControllerService(runner.getControllerService(KERBEROS_SERVICE_ID));

        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, METASTORE_URI);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.KERBEROS_USER_SERVICE, KERBEROS_SERVICE_ID);
        runner.enableControllerService(catalogService);

        assertNotNull(catalogService.getCatalog());

        runner.disableControllerService(catalogService);

        verify(kerberosUser).logout();
        assertFalse(runner.isControllerServiceEnabled(catalogService));
    }

    @Test
    void testEnableFailsWhenKerberosLoginFails() throws InitializationException {
        final KerberosUser kerberosUser = mock(KerberosUser.class);
        doThrow(new KerberosLoginException("Login failed")).when(kerberosUser).login();
        setKerberosUserService(kerberosUser);

        assertThrows(AssertionError.class, () -> runner.enableControllerService(catalogService));

        assertNull(catalogService.getCatalog());
        verify(kerberosUser).logout();
    }

    @Test
    void testDisabledWhenKerberosLogoutFails() throws InitializationException {
        final KerberosUser kerberosUser = mock(KerberosUser.class);
        doThrow(new KerberosLoginException("Logout failed")).when(kerberosUser).logout();
        setKerberosUserService(kerberosUser);
        runner.enableControllerService(catalogService);

        runner.disableControllerService(catalogService);

        assertFalse(runner.isControllerServiceEnabled(catalogService));
        assertNull(catalogService.getCatalog());
    }

    @Test
    void testClientPoolCacheKeyAppendedToDynamicProperty() {
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, METASTORE_URI);
        runner.setProperty(catalogService, CLIENT_POOL_CACHE_KEYS_PROPERTY, "ugi");
        runner.enableControllerService(catalogService);

        final HiveCatalog hiveCatalog = catalogService.getHiveCatalog();
        final String cacheKey = hiveCatalog.getConf().get(CLIENT_POOL_CACHE_KEY_PROPERTY);

        assertNotNull(cacheKey);
        assertTrue(cacheKey.startsWith(SERVICE_ID), cacheKey);
    }

    @Test
    void testGetCatalogRunsInServiceContext() {
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, METASTORE_URI);
        runner.enableControllerService(catalogService);

        final Catalog catalog = catalogService.getCatalog();

        assertTrue(Proxy.isProxyClass(catalog.getClass()));
        assertEquals(SERVICE_ID, catalog.name());
    }

    @Test
    void testOzoneFileSystemConfigured() {
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, METASTORE_URI);
        runner.enableControllerService(catalogService);

        assertEquals(OZONE_FILE_SYSTEM_CLASS, catalogService.getHiveCatalog().getConf().get(OZONE_FILE_SYSTEM_PROPERTY));
    }

    @Test
    void testOzoneFileSystemFromConfigurationResourcesPreserved() throws IOException {
        final Path configuration = writeConfiguration(OZONE_FILE_SYSTEM_PROPERTY, CUSTOM_FILE_SYSTEM_CLASS);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, METASTORE_URI);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, configuration.toString());
        runner.enableControllerService(catalogService);

        assertEquals(CUSTOM_FILE_SYSTEM_CLASS, catalogService.getHiveCatalog().getConf().get(OZONE_FILE_SYSTEM_PROPERTY));
    }

    @Test
    void testEnableFailsWithInvalidCatalogPropertyLogsOut() throws InitializationException {
        final KerberosUser kerberosUser = mock(KerberosUser.class);
        setKerberosUserService(kerberosUser);
        runner.setProperty(catalogService, FILE_IO_PROPERTY, MISSING_FILE_IO_CLASS);

        assertThrows(AssertionError.class, () -> runner.enableControllerService(catalogService));

        assertNull(catalogService.getCatalog());
        verify(kerberosUser).logout();
    }

    private void setKerberosUserService(final KerberosUser kerberosUser) throws InitializationException {
        runner.addControllerService(KERBEROS_SERVICE_ID, new MockKerberosUserService(kerberosUser));
        runner.enableControllerService(runner.getControllerService(KERBEROS_SERVICE_ID));

        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, METASTORE_URI);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.KERBEROS_USER_SERVICE, KERBEROS_SERVICE_ID);
    }

    private ConfigurationContext getContextWithKerberosUserService(final KerberosUser kerberosUser) throws InitializationException {
        runner.addControllerService(KERBEROS_SERVICE_ID, new MockKerberosUserService(kerberosUser));
        runner.enableControllerService(runner.getControllerService(KERBEROS_SERVICE_ID));

        final Map<PropertyDescriptor, String> properties = Map.of(
                HiveMetastoreIcebergCatalog.METASTORE_URI, METASTORE_URI,
                HiveMetastoreIcebergCatalog.KERBEROS_USER_SERVICE, KERBEROS_SERVICE_ID
        );

        return new MockConfigurationContext(catalogService, properties, runner.getProcessContext().getControllerServiceLookup(), null);
    }

    private static class MockKerberosUserService extends AbstractControllerService implements KerberosUserService {
        private final KerberosUser kerberosUser;

        private MockKerberosUserService(final KerberosUser kerberosUser) {
            this.kerberosUser = kerberosUser;
        }

        @Override
        public KerberosUser createKerberosUser() {
            return kerberosUser;
        }
    }

    private Path writeConfiguration(final String name, final String value) throws IOException {
        final Path configuration = tempDirectory.resolve("%s-site.xml".formatted(name.hashCode()));
        Files.writeString(configuration, CONFIGURATION_FORMAT.formatted(name, value));
        return configuration;
    }
}
