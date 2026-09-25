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

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Exercises the Hive Metastore client path against an address where no Metastore is listening. The Thrift client,
 * HiveConf and the Iceberg client pool are created for real, so missing runtime dependencies surface as failures here
 * instead of on a cluster. A Metastore backed by a database is out of scope: the Derby driver required for an
 * in-process Metastore is banned in this project.
 */
class HiveMetastoreIcebergCatalogClientTest {
    private static final String SERVICE_ID = "hive-metastore-catalog";

    private static final String METASTORE_URI_FORMAT = "thrift://localhost:%d";

    private static final String FILE_IO_FIELD = "fileIO";

    private static final String CLIENTS_FIELD = "clients";

    private static final String CLIENT_POOL_METHOD = "clientPool";

    private static final String SECOND_SERVICE_ID = "hive-metastore-catalog-second";

    private static final String CONFIGURATION_STEP = "Catalog Configuration";

    private static final String CONNECTION_STEP = "Metastore Connection";

    private static final String MALFORMED_CONFIGURATION = "<configuration><property><name>unclosed";

    private static final Namespace NAMESPACE = Namespace.empty();

    @TempDir
    private Path tempDirectory;

    private TestRunner runner;

    private HiveMetastoreIcebergCatalog catalogService;

    private String metastoreUri;

    private Path hiveConfiguration;

    @BeforeEach
    void setCatalogService() throws InitializationException, IOException {
        metastoreUri = METASTORE_URI_FORMAT.formatted(getUnusedPort());

        final Map<String, String> configurationProperties = new LinkedHashMap<>();
        configurationProperties.put("hive.metastore.warehouse.dir", tempDirectory.toUri().toString());
        configurationProperties.put("hive.metastore.failure.retries", "0");
        configurationProperties.put("hive.metastore.client.connect.retry.delay", "0s");
        configurationProperties.put("hive.metastore.connect.retries", "1");
        configurationProperties.put("hive.metastore.client.socket.timeout", "2s");
        hiveConfiguration = writeConfiguration("hive-site.xml", configurationProperties);

        catalogService = new HiveMetastoreIcebergCatalog();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService(SERVICE_ID, catalogService);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, metastoreUri);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, hiveConfiguration.toString());
    }

    @AfterEach
    void disableCatalogService() {
        if (runner.isControllerServiceEnabled(catalogService)) {
            runner.disableControllerService(catalogService);
        }
    }

    @Test
    void testMetastoreCallFailsWithConnectionErrorNotLinkageError() {
        runner.enableControllerService(catalogService);
        final HiveCatalog catalog = assertInstanceOf(HiveCatalog.class, catalogService.getCatalog());

        // A LinkageError is not an Exception: an unresolved Hive Metastore client dependency fails the assertion
        final Exception e = assertThrows(Exception.class, () -> catalog.listNamespaces(NAMESPACE));

        assertMetastoreConnectionFailure(e);
    }

    @Test
    void testCatalogFileIOIsHadoopFileIO() throws ReflectiveOperationException {
        runner.enableControllerService(catalogService);

        final HiveCatalog hiveCatalog = assertInstanceOf(HiveCatalog.class, catalogService.getCatalog());
        final Field fileIOField = HiveCatalog.class.getDeclaredField(FILE_IO_FIELD);
        fileIOField.setAccessible(true);

        assertInstanceOf(HadoopFileIO.class, fileIOField.get(hiveCatalog));
    }

    @Test
    void testVerifyReportsConnectionFailedForUnavailableMetastore() {
        final List<ConfigVerificationResult> results = verify();

        assertEquals(2, results.size());
        assertStepOutcome(results.get(0), CONFIGURATION_STEP, ConfigVerificationResult.Outcome.SUCCESSFUL);
        assertStepOutcome(results.get(1), CONNECTION_STEP, ConfigVerificationResult.Outcome.FAILED);
        assertMetastoreConnectionExplanation(results.get(1).getExplanation());
    }

    @Test
    void testVerifySkipsConnectionStepWhenConfigurationFails() throws IOException {
        final Path malformed = tempDirectory.resolve("malformed-site.xml");
        Files.writeString(malformed, MALFORMED_CONFIGURATION);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, malformed.toString());

        final List<ConfigVerificationResult> results = verify(malformed);

        assertEquals(2, results.size());
        assertStepOutcome(results.get(0), CONFIGURATION_STEP, ConfigVerificationResult.Outcome.FAILED);
        assertStepOutcome(results.get(1), CONNECTION_STEP, ConfigVerificationResult.Outcome.SKIPPED);

        final String explanation = results.get(0).getExplanation();
        assertFalse(explanation.toLowerCase().contains("noclassdeffound"), () -> "Missing runtime dependency reported: %s".formatted(explanation));
        assertTrue(explanation.contains(malformed.getFileName().toString()), () -> "Configuration file not named: %s".formatted(explanation));
        assertTrue(explanation.contains("caused by"), () -> "Cause chain not reported: %s".formatted(explanation));
    }

    @Test
    void testMetastoreUriListIsTrimmed() {
        final String spacedUriList = " %s , %s ".formatted(metastoreUri, metastoreUri);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, spacedUriList);
        runner.enableControllerService(catalogService);

        final HiveCatalog hiveCatalog = assertInstanceOf(HiveCatalog.class, catalogService.getCatalog());
        final String configuredUris = hiveCatalog.getConf().get("hive.metastore.uris");

        assertEquals("%s,%s".formatted(metastoreUri, metastoreUri), configuredUris);
    }

    @Test
    void testNotValidWhenExpressionLanguageResolvesToBlank() {
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, "${hive.metastore.uris.not.configured}");
        runner.removeProperty(catalogService, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES);

        runner.assertNotValid(catalogService);
    }

    @Test
    void testNotValidWhenConfigurationResourceIsMalformed() throws IOException {
        final Path malformed = tempDirectory.resolve("malformed-site.xml");
        Files.writeString(malformed, MALFORMED_CONFIGURATION);
        runner.removeProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, malformed.toString());

        runner.assertNotValid(catalogService);
    }

    @Test
    void testClientPoolNotSharedBetweenServiceInstances() throws Exception {
        runner.enableControllerService(catalogService);

        final HiveMetastoreIcebergCatalog secondService = new HiveMetastoreIcebergCatalog();
        runner.addControllerService(SECOND_SERVICE_ID, secondService);
        runner.setProperty(secondService, HiveMetastoreIcebergCatalog.METASTORE_URI, metastoreUri);
        runner.setProperty(secondService, HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, hiveConfiguration.toString());
        runner.enableControllerService(secondService);

        try {
            assertNotSame(getClientPool(catalogService), getClientPool(secondService));
        } finally {
            runner.disableControllerService(secondService);
        }
    }

    private Object getClientPool(final HiveMetastoreIcebergCatalog service) throws ReflectiveOperationException {
        final HiveCatalog hiveCatalog = assertInstanceOf(HiveCatalog.class, service.getCatalog());

        final Field clientsField = HiveCatalog.class.getDeclaredField(CLIENTS_FIELD);
        clientsField.setAccessible(true);
        final Object cachedClientPool = clientsField.get(hiveCatalog);

        final Method clientPoolMethod = cachedClientPool.getClass().getDeclaredMethod(CLIENT_POOL_METHOD);
        clientPoolMethod.setAccessible(true);
        return clientPoolMethod.invoke(cachedClientPool);
    }

    private List<ConfigVerificationResult> verify() {
        return verify(hiveConfiguration);
    }

    private List<ConfigVerificationResult> verify(final Path configurationResource) {
        final Map<PropertyDescriptor, String> properties = Map.of(
                HiveMetastoreIcebergCatalog.METASTORE_URI, metastoreUri,
                HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, configurationResource.toString()
        );
        final ConfigurationContext context = new MockConfigurationContext(catalogService, properties, null, null);

        return catalogService.verify(context, runner.getLogger(), Map.of());
    }

    private void assertStepOutcome(final ConfigVerificationResult result, final String step, final ConfigVerificationResult.Outcome outcome) {
        assertEquals(step, result.getVerificationStepName());
        assertEquals(outcome, result.getOutcome());
        assertNotNull(result.getExplanation());
        assertFalse(result.getExplanation().isBlank());
    }

    private void assertMetastoreConnectionFailure(final Exception e) {
        final String explanation = getMessages(e);
        assertMetastoreConnectionExplanation(explanation);
    }

    private void assertMetastoreConnectionExplanation(final String explanation) {
        final String lowerCased = explanation.toLowerCase();
        assertFalse(lowerCased.contains("noclassdeffound"), () -> "Missing runtime dependency reported: %s".formatted(explanation));
        assertFalse(lowerCased.contains("classnotfound"), () -> "Missing runtime dependency reported: %s".formatted(explanation));
        assertTrue(
                lowerCased.contains("connect") || lowerCased.contains("metastore") || lowerCased.contains("transport"),
                () -> "Metastore connection failure not reported: %s".formatted(explanation)
        );
    }

    private String getMessages(final Throwable e) {
        final StringBuilder messages = new StringBuilder();
        Throwable current = e;
        while (current != null && messages.length() < 4096) {
            messages.append(current.getClass().getName()).append(' ').append(current.getMessage()).append(' ');
            current = current.getCause() == current ? null : current.getCause();
        }
        return messages.toString();
    }

    private Path writeConfiguration(final String fileName, final Map<String, String> properties) throws IOException {
        final StringBuilder configuration = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration>");
        properties.forEach((name, value) -> configuration
                .append("<property><name>").append(name).append("</name><value>").append(value).append("</value></property>"));
        configuration.append("</configuration>");

        final Path configurationFile = tempDirectory.resolve(fileName);
        Files.writeString(configurationFile, configuration.toString());
        return configurationFile;
    }

    private int getUnusedPort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return serverSocket.getLocalPort();
        }
    }
}
