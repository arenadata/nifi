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

import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.catalog.Namespace;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.util.MockConfigurationContext;
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
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.EnabledIfDockerAvailable;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the Controller Service against a Hive Metastore that requires Kerberos SASL authentication. The Key Distribution
 * Center runs in the test process. The Metastore container reaches it through the Docker host gateway, which unlike
 * Testcontainers exposed host ports does not attach later containers of other tests to a shared network. The service
 * reaches it through a TCP relay that the test stops to simulate a Key Distribution Center outage without losing
 * principals. Tickets live for a short time and Metastore connections are reopened after a few seconds, so that new
 * SASL connections need fresh credentials during the tests. The Metastore records the authenticated user as the owner
 * of created databases, which shows the identity presented by the service. Skipped when Docker is not available.
 */
@EnabledIfDockerAvailable
@Timeout(value = 15, unit = TimeUnit.MINUTES)
class HiveMetastoreIcebergCatalogKerberosTest {
    private static final String IMAGE_PROPERTY = "hive.metastore.image";

    private static final String IMAGE_DEFAULT = "apache/hive:4.2.1";

    private static final String KRB5_CONF_PROPERTY = "java.security.krb5.conf";

    private static final String HOST_INTERNAL = "host.docker.internal";

    private static final String HOST_GATEWAY = "host-gateway";

    private static final String LOCALHOST = "localhost";

    private static final String REALM = "NIFI.COM";

    private static final Duration TICKET_LIFETIME = Duration.ofSeconds(30);

    private static final Duration EXPIRATION_MARGIN = Duration.ofSeconds(5);

    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);

    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);

    private static final int METASTORE_PORT = 9083;

    private static final String METASTORE_URI_FORMAT = "thrift://%s:%d";

    private static final String SERVICE_PRINCIPAL = "hive/localhost";

    private static final String FIRST_USER = "nifi-first";

    private static final String SECOND_USER = "nifi-second";

    private static final String PRINCIPAL_FORMAT = "%s@%s";

    private static final String CONTAINER_KEYTAB = "/opt/hive/conf/hive.keytab";

    private static final String CUSTOM_CONF_DIR = "/opt/hive/custom-conf";

    private static final String KRB5_CONF_FORMAT = """
            [libdefaults]
                default_realm = %1$s
                dns_canonicalize_hostname = false
                rdns = false
                udp_preference_limit = 1
                max_retries = 1
                kdc_timeout = 5000
            [realms]
                %1$s = {
                    kdc = %2$s:%3$d
                }
            """;

    private static final String CONFIGURATION_FORMAT = """
            <?xml version="1.0" encoding="UTF-8"?>
            <configuration>%s</configuration>""";

    private static final String PROPERTY_FORMAT = "<property><name>%s</name><value>%s</value></property>";

    private static final String SERVICE_ID_FORMAT = "hive-metastore-catalog-%d";

    private static final String KERBEROS_SERVICE_ID_FORMAT = "kerberos-user-service-%d";

    private static final String NAMESPACE_FORMAT = "nifi_kerberos_%d";

    private static final String CONFIGURATION_STEP = "Catalog Configuration";

    private static final AtomicInteger COUNTER = new AtomicInteger();

    private static String krb5Configuration;

    private static MiniKdc kdc;

    private static KdcRelay kdcRelay;

    private static Path warehouse;

    private static Path clientConfiguration;

    private static File firstKeytab;

    private static File secondKeytab;

    private static GenericContainer<?> metastore;

    private TestRunner runner;

    private final List<HiveMetastoreIcebergCatalog> services = new ArrayList<>();

    private final Map<HiveMetastoreIcebergCatalog, Map<PropertyDescriptor, String>> serviceProperties = new HashMap<>();

    @BeforeAll
    static void startServices(@TempDir final Path directory) throws Exception {
        krb5Configuration = System.getProperty(KRB5_CONF_PROPERTY);

        final Properties properties = MiniKdc.createConf();
        properties.setProperty(MiniKdc.ORG_NAME, "NIFI");
        properties.setProperty(MiniKdc.ORG_DOMAIN, "COM");
        properties.setProperty(MiniKdc.KDC_BIND_ADDRESS, "0.0.0.0");
        properties.setProperty(MiniKdc.MIN_TICKET_LIFETIME, "1");
        properties.setProperty(MiniKdc.MAX_TICKET_LIFETIME, Long.toString(TICKET_LIFETIME.toSeconds()));
        properties.setProperty(MiniKdc.MAX_RENEWABLE_LIFETIME, Long.toString(TICKET_LIFETIME.toSeconds()));
        kdc = new MiniKdc(properties, directory.toFile());
        kdc.start();

        final File serviceKeytab = directory.resolve("hive.keytab").toFile();
        kdc.createPrincipal(serviceKeytab, SERVICE_PRINCIPAL);
        firstKeytab = directory.resolve("first.keytab").toFile();
        kdc.createPrincipal(firstKeytab, FIRST_USER);
        secondKeytab = directory.resolve("second.keytab").toFile();
        kdc.createPrincipal(secondKeytab, SECOND_USER);

        kdcRelay = new KdcRelay(kdc.getPort());
        kdcRelay.start();

        final Path clientKrb5 = directory.resolve("krb5.conf");
        Files.writeString(clientKrb5, KRB5_CONF_FORMAT.formatted(REALM, LOCALHOST, kdcRelay.getPort()));
        System.setProperty(KRB5_CONF_PROPERTY, clientKrb5.toString());

        warehouse = Files.createTempDirectory("nifi-iceberg-kerberos-warehouse");
        Files.setPosixFilePermissions(warehouse, PosixFilePermissions.fromString("rwxrwxrwx"));

        clientConfiguration = directory.resolve("kerberos-site.xml");
        Files.writeString(clientConfiguration, getConfiguration(Map.of(
                "hadoop.security.authentication", "kerberos",
                "hadoop.kerberos.min.seconds.before.relogin", "1",
                "hive.metastore.sasl.enabled", "true",
                "hive.metastore.kerberos.principal", PRINCIPAL_FORMAT.formatted(SERVICE_PRINCIPAL, REALM),
                "hive.metastore.client.socket.lifetime", "5s",
                "hive.metastore.warehouse.dir", warehouse.toUri().toString()
        )));

        metastore = new GenericContainer<>(DockerImageName.parse(System.getProperty(IMAGE_PROPERTY, IMAGE_DEFAULT)))
                .withEnv("SERVICE_NAME", "metastore")
                .withEnv("HIVE_CUSTOM_CONF_DIR", CUSTOM_CONF_DIR)
                .withExtraHost(HOST_INTERNAL, HOST_GATEWAY)
                .withExposedPorts(METASTORE_PORT)
                .withFileSystemBind(warehouse.toString(), warehouse.toString(), BindMode.READ_WRITE)
                .withCopyToContainer(Transferable.of(KRB5_CONF_FORMAT.formatted(REALM, HOST_INTERNAL, kdc.getPort())), "/etc/krb5.conf")
                .withCopyToContainer(Transferable.of(Files.readAllBytes(serviceKeytab.toPath()), Integer.parseInt("644", 8)), CONTAINER_KEYTAB)
                .withCopyToContainer(Transferable.of(getConfiguration(Map.of(
                        "hive.metastore.sasl.enabled", "true",
                        "hive.metastore.kerberos.principal", PRINCIPAL_FORMAT.formatted(SERVICE_PRINCIPAL, REALM),
                        "hive.metastore.kerberos.keytab.file", CONTAINER_KEYTAB
                ))), CUSTOM_CONF_DIR + "/hive-site.xml")
                .withCopyToContainer(Transferable.of(getConfiguration(Map.of(
                        "hadoop.security.authentication", "kerberos"
                ))), CUSTOM_CONF_DIR + "/core-site.xml")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT));
        metastore.start();
    }

    @AfterAll
    static void stopServices() {
        if (metastore != null) {
            metastore.stop();
        }
        if (kdcRelay != null) {
            kdcRelay.stop();
        }
        if (kdc != null) {
            kdc.stop();
        }

        UserGroupInformation.reset();
        if (krb5Configuration == null) {
            System.clearProperty(KRB5_CONF_PROPERTY);
        } else {
            System.setProperty(KRB5_CONF_PROPERTY, krb5Configuration);
        }
    }

    @BeforeEach
    void setRunner() throws IOException {
        kdcRelay.start();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
    }

    @AfterEach
    void disableServices() throws IOException {
        kdcRelay.start();
        services.stream().filter(runner::isControllerServiceEnabled).forEach(runner::disableControllerService);
    }

    @Test
    void testDatabaseCreatedAsKerberosPrincipal() throws Exception {
        final HiveMetastoreCatalog catalog = enableService(FIRST_USER, firstKeytab);

        final Namespace namespace = createNamespace(catalog);

        assertEquals(FIRST_USER, getOwner(catalog, namespace));
    }

    @Test
    void testServicesKeepOwnPrincipals() throws Exception {
        final HiveMetastoreCatalog first = enableService(FIRST_USER, firstKeytab);
        final HiveMetastoreCatalog second = enableService(SECOND_USER, secondKeytab);

        final Namespace firstNamespace = createNamespace(first);
        final Namespace secondNamespace = createNamespace(second);
        final Namespace firstAgainNamespace = createNamespace(first);

        assertEquals(FIRST_USER, getOwner(first, firstNamespace));
        assertEquals(SECOND_USER, getOwner(second, secondNamespace));
        assertEquals(FIRST_USER, getOwner(first, firstAgainNamespace));
    }

    @Test
    void testEnableFailsWhileKdcUnavailableAndSucceedsAfterwards() throws Exception {
        final HiveMetastoreIcebergCatalog service = addService(FIRST_USER, firstKeytab);

        kdcRelay.stop();
        assertThrows(AssertionError.class, () -> runner.enableControllerService(service));

        kdcRelay.start();
        runner.enableControllerService(service);
        assertListNamespacesAvailable(service.getHiveCatalog());
    }

    @Test
    void testVerifyReportsKerberosFailures() throws Exception {
        final HiveMetastoreIcebergCatalog unavailableService = addService(FIRST_USER, firstKeytab);
        kdcRelay.stop();
        assertConfigurationFailed(unavailableService);
        kdcRelay.start();

        final HiveMetastoreIcebergCatalog wrongKeytabService = addService(FIRST_USER, secondKeytab);
        assertConfigurationFailed(wrongKeytabService);
    }

    @Test
    void testMetastoreReconnectsAfterTicketExpiration() throws Exception {
        final HiveMetastoreCatalog catalog = enableService(FIRST_USER, firstKeytab);
        assertListNamespacesAvailable(catalog);

        Thread.sleep(TICKET_LIFETIME.plus(EXPIRATION_MARGIN).toMillis());

        final Namespace namespace = createNamespace(catalog);
        assertEquals(FIRST_USER, getOwner(catalog, namespace));
    }

    @Test
    void testOperationsRecoverAfterKdcOutage() throws Exception {
        final HiveMetastoreCatalog catalog = enableService(FIRST_USER, firstKeytab);
        assertListNamespacesAvailable(catalog);

        kdcRelay.stop();
        Thread.sleep(TICKET_LIFETIME.plus(EXPIRATION_MARGIN).toMillis());
        assertThrows(RuntimeException.class, () -> catalog.listNamespaces(Namespace.empty()));

        kdcRelay.start();
        assertListNamespacesAvailable(catalog);
        final Namespace namespace = createNamespace(catalog);
        assertEquals(FIRST_USER, getOwner(catalog, namespace));
    }

    private HiveMetastoreCatalog enableService(final String user, final File keytab) throws Exception {
        final HiveMetastoreIcebergCatalog service = addService(user, keytab);
        runner.enableControllerService(service);
        final HiveMetastoreCatalog catalog = service.getHiveCatalog();
        assertListNamespacesAvailable(catalog);
        return catalog;
    }

    private HiveMetastoreIcebergCatalog addService(final String user, final File keytab) throws InitializationException {
        final int index = COUNTER.incrementAndGet();
        final String kerberosServiceId = KERBEROS_SERVICE_ID_FORMAT.formatted(index);
        final KerberosUserService kerberosUserService = new KeytabUserService(PRINCIPAL_FORMAT.formatted(user, REALM), keytab.getAbsolutePath());
        runner.addControllerService(kerberosServiceId, kerberosUserService);
        runner.enableControllerService(kerberosUserService);

        final Map<PropertyDescriptor, String> properties = Map.of(
                HiveMetastoreIcebergCatalog.METASTORE_URI, getMetastoreUri(),
                HiveMetastoreIcebergCatalog.HADOOP_CONFIGURATION_RESOURCES, clientConfiguration.toString(),
                HiveMetastoreIcebergCatalog.KERBEROS_USER_SERVICE, kerberosServiceId,
                HiveMetastoreIcebergCatalog.METASTORE_CONNECTION_ATTEMPTS, "1",
                HiveMetastoreIcebergCatalog.METASTORE_CONNECTION_RETRY_DELAY, "0 sec"
        );

        final HiveMetastoreIcebergCatalog service = new HiveMetastoreIcebergCatalog();
        runner.addControllerService(SERVICE_ID_FORMAT.formatted(index), service);
        properties.forEach((descriptor, value) -> runner.setProperty(service, descriptor, value));
        services.add(service);
        serviceProperties.put(service, properties);
        return service;
    }

    private void assertConfigurationFailed(final HiveMetastoreIcebergCatalog service) {
        final ConfigurationContext context = new MockConfigurationContext(service, serviceProperties.get(service), runner.getProcessContext().getControllerServiceLookup(), null);

        final List<ConfigVerificationResult> results = service.verify(context, runner.getLogger(), Map.of());

        final ConfigVerificationResult configurationResult = results.getFirst();
        assertEquals(CONFIGURATION_STEP, configurationResult.getVerificationStepName());
        assertEquals(ConfigVerificationResult.Outcome.FAILED, configurationResult.getOutcome(), configurationResult.getExplanation());
    }

    private Namespace createNamespace(final HiveMetastoreCatalog catalog) throws IOException {
        final Namespace namespace = Namespace.of(NAMESPACE_FORMAT.formatted(COUNTER.incrementAndGet()));
        Files.createDirectory(warehouse.resolve("%s.db".formatted(namespace.level(0))));
        catalog.createNamespace(namespace);
        return namespace;
    }

    private String getOwner(final HiveMetastoreCatalog catalog, final Namespace namespace) throws Exception {
        return catalog.getClientPool().run(client -> client.getDatabase(namespace.level(0)).getOwnerName());
    }

    private static void assertListNamespacesAvailable(final HiveMetastoreCatalog catalog) throws InterruptedException {
        final long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
        while (true) {
            try {
                assertTrue(catalog.listNamespaces(Namespace.empty()).contains(Namespace.of("default")));
                return;
            } catch (final RuntimeException e) {
                if (System.nanoTime() > deadline) {
                    throw e;
                }
                Thread.sleep(POLL_INTERVAL.toMillis());
            }
        }
    }

    private static String getMetastoreUri() {
        return METASTORE_URI_FORMAT.formatted(LOCALHOST, metastore.getMappedPort(METASTORE_PORT));
    }

    private static String getConfiguration(final Map<String, String> properties) {
        final StringBuilder configuration = new StringBuilder();
        properties.forEach((name, value) -> configuration.append(PROPERTY_FORMAT.formatted(name, value)));
        return CONFIGURATION_FORMAT.formatted(configuration);
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

    /**
     * TCP relay to the Key Distribution Center on a fixed local port, stopped and started again to simulate an outage
     */
    private static class KdcRelay {
        private final int targetPort;

        private final int port;

        private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();

        private volatile ServerSocket serverSocket;

        private KdcRelay(final int targetPort) throws IOException {
            this.targetPort = targetPort;
            try (ServerSocket socket = new ServerSocket(0)) {
                this.port = socket.getLocalPort();
            }
        }

        int getPort() {
            return port;
        }

        synchronized void start() throws IOException {
            if (serverSocket != null) {
                return;
            }
            final ServerSocket socket = new ServerSocket();
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
            serverSocket = socket;
            Thread.ofVirtual().start(() -> accept(socket));
        }

        synchronized void stop() {
            final ServerSocket socket = serverSocket;
            serverSocket = null;
            if (socket != null) {
                close(socket);
            }
            sockets.forEach(KdcRelay::close);
            sockets.clear();
        }

        private void accept(final ServerSocket socket) {
            while (!socket.isClosed()) {
                try {
                    final Socket client = socket.accept();
                    final Socket target = new Socket(InetAddress.getLoopbackAddress(), targetPort);
                    sockets.add(client);
                    sockets.add(target);
                    Thread.ofVirtual().start(() -> transfer(client, target));
                    Thread.ofVirtual().start(() -> transfer(target, client));
                } catch (final IOException e) {
                    return;
                }
            }
        }

        private void transfer(final Socket source, final Socket destination) {
            try (InputStream input = source.getInputStream(); OutputStream output = destination.getOutputStream()) {
                input.transferTo(output);
            } catch (final IOException e) {
                close(source);
            } finally {
                close(destination);
            }
        }

        private static void close(final Closeable closeable) {
            try {
                closeable.close();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
