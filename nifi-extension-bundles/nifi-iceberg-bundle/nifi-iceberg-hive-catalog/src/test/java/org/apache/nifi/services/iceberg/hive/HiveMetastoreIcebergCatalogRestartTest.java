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
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.EnabledIfDockerAvailable;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers Hive Metastore availability changes that happen on a cluster while the Controller Service stays enabled:
 * the Metastore starting after the service, and the Metastore restarting between operations. The Metastore is bound
 * to a fixed host port so that the replacement container keeps the configured URI. Skipped when Docker is not available.
 */
@EnabledIfDockerAvailable
@Timeout(value = 10, unit = TimeUnit.MINUTES)
class HiveMetastoreIcebergCatalogRestartTest {
    private static final String IMAGE_PROPERTY = "hive.metastore.image";

    private static final String IMAGE_DEFAULT = "apache/hive:4.2.1";

    private static final int METASTORE_PORT = 9083;

    private static final String METASTORE_URI_FORMAT = "thrift://localhost:%d";

    private static final String PORT_BINDING_FORMAT = "%d:%d";

    private static final String SERVICE_ID = "hive-metastore-catalog";

    private static final String DEFAULT_DATABASE = "default";

    private static final String WAITING_CONNECTION_ATTEMPTS = "90";

    private static final String WAITING_CONNECTION_RETRY_DELAY = "2 sec";

    private static final String SINGLE_CONNECTION_ATTEMPT = "1";

    private static final String NO_CONNECTION_RETRY_DELAY = "0 sec";

    private static final String CONNECTION_TIMEOUT = "5 sec";

    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);

    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);

    private TestRunner runner;

    private HiveMetastoreIcebergCatalog catalogService;

    private int metastorePort;

    private GenericContainer<?> metastore;

    @BeforeEach
    void setCatalogService() throws InitializationException, IOException {
        metastorePort = getUnusedPort();

        catalogService = new HiveMetastoreIcebergCatalog();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService(SERVICE_ID, catalogService);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_URI, METASTORE_URI_FORMAT.formatted(metastorePort));
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
    }

    @AfterEach
    void stopMetastore() {
        if (runner.isControllerServiceEnabled(catalogService)) {
            runner.disableControllerService(catalogService);
        }
        if (metastore != null) {
            metastore.stop();
        }
    }

    @Test
    void testCatalogConnectsToMetastoreStartedAfterService() throws InterruptedException {
        setConnectionRetries(WAITING_CONNECTION_ATTEMPTS, WAITING_CONNECTION_RETRY_DELAY);
        runner.enableControllerService(catalogService);
        final HiveMetastoreCatalog catalog = catalogService.getHiveCatalog();

        final CompletableFuture<Void> started = startMetastoreAsync();
        assertListNamespacesAvailable(catalog);
        started.join();
    }

    @Test
    void testCatalogReconnectsAfterMetastoreRestart() throws InterruptedException {
        setConnectionRetries(WAITING_CONNECTION_ATTEMPTS, WAITING_CONNECTION_RETRY_DELAY);
        startMetastore();
        runner.enableControllerService(catalogService);
        final HiveMetastoreCatalog catalog = catalogService.getHiveCatalog();
        assertListNamespacesAvailable(catalog);

        metastore.stop();
        final CompletableFuture<Void> restarted = startMetastoreAsync();
        assertListNamespacesAvailable(catalog);
        restarted.join();
    }

    @Test
    void testCatalogRecoversAfterMetastoreUnavailable() throws InterruptedException {
        setConnectionRetries(SINGLE_CONNECTION_ATTEMPT, NO_CONNECTION_RETRY_DELAY);
        startMetastore();
        runner.enableControllerService(catalogService);
        final HiveMetastoreCatalog catalog = catalogService.getHiveCatalog();
        assertListNamespacesAvailable(catalog);

        metastore.stop();
        assertThrows(RuntimeException.class, () -> catalog.listNamespaces(Namespace.empty()));

        startMetastore();
        assertListNamespacesAvailable(catalog);
    }

    private void setConnectionRetries(final String attempts, final String delay) {
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_CONNECTION_ATTEMPTS, attempts);
        runner.setProperty(catalogService, HiveMetastoreIcebergCatalog.METASTORE_CONNECTION_RETRY_DELAY, delay);
    }

    private CompletableFuture<Void> startMetastoreAsync() {
        return CompletableFuture.runAsync(this::startMetastore);
    }

    private void startMetastore() {
        final GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(System.getProperty(IMAGE_PROPERTY, IMAGE_DEFAULT)))
                .withEnv("SERVICE_NAME", "metastore")
                .withExposedPorts(METASTORE_PORT)
                .waitingFor(Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT));
        container.setPortBindings(List.of(PORT_BINDING_FORMAT.formatted(metastorePort, METASTORE_PORT)));
        metastore = container;
        container.start();
    }

    // Docker Desktop accepts connections on published ports before the Metastore listens, so a call can fail once the client treats that as connected
    private void assertListNamespacesAvailable(final HiveMetastoreCatalog catalog) throws InterruptedException {
        final long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
        while (true) {
            try {
                assertTrue(catalog.listNamespaces(Namespace.empty()).contains(Namespace.of(DEFAULT_DATABASE)));
                return;
            } catch (final RuntimeException e) {
                if (System.nanoTime() > deadline) {
                    throw e;
                }
                Thread.sleep(POLL_INTERVAL.toMillis());
            }
        }
    }

    private static int getUnusedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
