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
package org.apache.nifi.services.iceberg.hive.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.hive.HiveClientPool;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Hive Metastore with Kerberos SASL authentication as the hive service principal. Storage operations run as the client
 * through a proxy user, and the Ozone File System library is loaded from the auxiliary jars directory.
 */
public final class MetastoreComponent implements Startable {
    public static final String WAREHOUSE = "hdfs://%s:%d/warehouse".formatted(ClusterContext.HOST, ClusterContext.NAME_NODE_PORT);

    public static final String METASTORE_URI = "thrift://%s:%d".formatted(ClusterContext.HOST, ClusterContext.METASTORE_PORT);

    private static final String IMAGE_PROPERTY = "hive.metastore.image";

    private static final String IMAGE_DEFAULT = "apache/hive:4.2.1";

    private static final String OZONE_FILE_SYSTEM_LIBRARY = "ozone-filesystem-hadoop3.jar";

    private static final String OZONE_FILE_SYSTEM_LIBRARY_PROPERTY = "ozone.filesystem.library";

    private static final String OZONE_FILE_SYSTEM_LIBRARY_DEFAULT = "target/ozone-filesystem/" + OZONE_FILE_SYSTEM_LIBRARY;

    private static final String AUXILIARY_JARS_DIRECTORY = "/opt/hive/ozone-lib";

    private static final String CONFIGURATION_DIRECTORY = "/opt/hive/custom-conf";

    private static final String CONFIGURATION_FILE_FORMAT = CONFIGURATION_DIRECTORY + "/%s";

    private static final String CORE_SITE = "core-site.xml";

    private static final String HIVE_SITE = "hive-site.xml";

    private static final int READABLE_FILE_MODE = Integer.parseInt("644", 8);

    private static final String SERVICE_NAME_VARIABLE = "SERVICE_NAME";

    private static final String SERVICE_NAME = "metastore";

    private static final String AUXILIARY_JARS_VARIABLE = "HIVE_AUX_JARS_PATH";

    private static final String CUSTOM_CONFIGURATION_VARIABLE = "HIVE_CUSTOM_CONF_DIR";

    private static final String PORT_VARIABLE = "METASTORE_PORT";

    private static final String SERVICE_OPTIONS_VARIABLE = "SERVICE_OPTS";

    private static final String SERVICE_OPTIONS = "-Djava.security.krb5.conf=%s -Dsun.security.krb5.rcache=none".formatted(ClusterContext.KRB5_CONF_PATH);

    private static final String URIS_PROPERTY = "hive.metastore.uris";

    private static final String SASL_ENABLED_PROPERTY = "hive.metastore.sasl.enabled";

    private static final String PRINCIPAL_PROPERTY = "hive.metastore.kerberos.principal";

    private static final String KEYTAB_PROPERTY = "hive.metastore.kerberos.keytab.file";

    private static final String CONNECT_RETRIES_PROPERTY = "hive.metastore.connect.retries";

    private static final String CONNECT_RETRY_DELAY_PROPERTY = "hive.metastore.client.connect.retry.delay";

    private static final String SOCKET_TIMEOUT_PROPERTY = "hive.metastore.client.socket.timeout";

    private static final String CONNECT_RETRIES = "1";

    private static final String CONNECT_RETRY_DELAY = "1s";

    private static final String SOCKET_TIMEOUT = "20s";

    private static final String ENABLED = "true";

    private static final String DISABLED = "false";

    private static final String SERVLET_DISABLED = "-1";

    private static final String DEFAULT_DATABASE = "default";

    private static final String LOG_PREFIX = "metastore";

    private static final int LOG_TAIL_LINES = 50;

    private final ClusterContext context;

    private final GenericContainer<?> container;

    public MetastoreComponent(final ClusterContext context) {
        this.context = context;

        final Path ozoneFileSystemLibrary = Path.of(System.getProperty(OZONE_FILE_SYSTEM_LIBRARY_PROPERTY, OZONE_FILE_SYSTEM_LIBRARY_DEFAULT)).toAbsolutePath();
        if (!Files.isRegularFile(ozoneFileSystemLibrary)) {
            throw new IllegalStateException("Ozone File System library [%s] not found: run the Maven build to copy it".formatted(ozoneFileSystemLibrary));
        }

        final Map<String, String> clientProperties = new LinkedHashMap<>(HdfsComponent.clientProperties());
        clientProperties.putAll(OzoneComponent.clientProperties());
        final String coreSite = ClusterFiles.hadoopXml(context.coreSite(clientProperties));

        this.container = new GenericContainer<>(DockerImageName.parse(System.getProperty(IMAGE_PROPERTY, IMAGE_DEFAULT)))
                .withEnv(SERVICE_NAME_VARIABLE, SERVICE_NAME)
                .withEnv(PORT_VARIABLE, Integer.toString(ClusterContext.METASTORE_PORT))
                .withEnv(AUXILIARY_JARS_VARIABLE, AUXILIARY_JARS_DIRECTORY)
                .withEnv(CUSTOM_CONFIGURATION_VARIABLE, CONFIGURATION_DIRECTORY)
                .withEnv(SERVICE_OPTIONS_VARIABLE, SERVICE_OPTIONS)
                .withCopyToContainer(Transferable.of(context.krb5Conf()), ClusterContext.KRB5_CONF_PATH)
                .withCopyToContainer(MountableFile.forHostPath(ozoneFileSystemLibrary, READABLE_FILE_MODE), "%s/%s".formatted(AUXILIARY_JARS_DIRECTORY, OZONE_FILE_SYSTEM_LIBRARY))
                .withCopyToContainer(Transferable.of(coreSite), CONFIGURATION_FILE_FORMAT.formatted(CORE_SITE))
                .withCopyToContainer(Transferable.of(ClusterFiles.hadoopXml(getServerProperties(context))), CONFIGURATION_FILE_FORMAT.formatted(HIVE_SITE))
                .withFileSystemBind(context.keytabDirectory().toString(), ClusterContext.KEYTAB_DIRECTORY, BindMode.READ_ONLY)
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withNetworkMode(context.networkMode()))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(MetastoreComponent.class)).withPrefix(LOG_PREFIX));
    }

    /**
     * Start the Hive Metastore and wait until listing databases through Kerberos SASL as the hive principal succeeds
     */
    @Override
    public void start() {
        container.start();
        try {
            awaitMetastore();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Waiting for Hive Metastore interrupted", e);
        }
    }

    @Override
    public void stop() {
        container.stop();
    }

    /**
     * Hive Metastore client properties to be combined with the shared core site properties
     *
     * @return Hive Metastore client properties
     */
    public static Map<String, String> clientProperties() {
        final Map<String, String> properties = new LinkedHashMap<>();
        properties.put(URIS_PROPERTY, METASTORE_URI);
        properties.put(SASL_ENABLED_PROPERTY, ENABLED);
        properties.put(PRINCIPAL_PROPERTY, ClusterContext.HIVE_PRINCIPAL);
        return Collections.unmodifiableMap(properties);
    }

    private void awaitMetastore() throws InterruptedException {
        final Configuration configuration = new Configuration();
        context.coreSite(clientProperties()).forEach(configuration::set);
        configuration.set(CONNECT_RETRIES_PROPERTY, CONNECT_RETRIES);
        configuration.set(CONNECT_RETRY_DELAY_PROPERTY, CONNECT_RETRY_DELAY);
        configuration.set(SOCKET_TIMEOUT_PROPERTY, SOCKET_TIMEOUT);

        final UserGroupInformation hive;
        try {
            UserGroupInformation.setConfiguration(configuration);
            hive = UserGroupInformation.loginUserFromKeytabAndReturnUGI(ClusterContext.HIVE_PRINCIPAL, context.keytab(ClusterContext.HIVE_KEYTAB).toString());
        } catch (final IOException e) {
            throw new UncheckedIOException("Kerberos login failed for [%s]".formatted(ClusterContext.HIVE_PRINCIPAL), e);
        }

        final AtomicBoolean stopped = new AtomicBoolean();
        ClusterFiles.waitUntil("Hive Metastore", ClusterContext.STARTUP_TIMEOUT, () -> {
            if (!container.isRunning()) {
                stopped.set(true);
                return;
            }
            final List<String> databases = hive.doAs((PrivilegedExceptionAction<List<String>>) () -> {
                try (HiveClientPool clientPool = new HiveClientPool(1, configuration)) {
                    return clientPool.run(IMetaStoreClient::getAllDatabases);
                }
            });
            if (!databases.contains(DEFAULT_DATABASE)) {
                throw new IllegalStateException("Database [%s] not found in %s".formatted(DEFAULT_DATABASE, databases));
            }
        });

        if (stopped.get()) {
            final List<String> logs = container.getLogs().lines().toList();
            final String tail = String.join(System.lineSeparator(), logs.subList(Math.max(0, logs.size() - LOG_TAIL_LINES), logs.size()));
            throw new IllegalStateException("Hive Metastore container stopped:%n%s".formatted(tail));
        }
    }

    private static Map<String, String> getServerProperties(final ClusterContext context) {
        final Map<String, String> properties = new LinkedHashMap<>();
        properties.put(SASL_ENABLED_PROPERTY, ENABLED);
        properties.put(PRINCIPAL_PROPERTY, ClusterContext.HIVE_PRINCIPAL);
        properties.put(KEYTAB_PROPERTY, context.containerKeytab(ClusterContext.HIVE_KEYTAB));
        properties.put("hive.metastore.port", Integer.toString(ClusterContext.METASTORE_PORT));
        properties.put("hive.metastore.warehouse.dir", WAREHOUSE);
        properties.put("hive.metastore.warehouse.external.dir", WAREHOUSE);
        properties.put("hive.metastore.event.db.notification.api.auth", DISABLED);
        properties.put("hive.compactor.initiator.on", DISABLED);
        properties.put("hive.compactor.cleaner.on", DISABLED);
        properties.put("hive.metastore.catalog.servlet.port", SERVLET_DISABLED);
        properties.put("hive.metastore.properties.servlet.port", SERVLET_DISABLED);
        return properties;
    }
}
