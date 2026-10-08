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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Trino server with an Iceberg catalog on the Kerberized Hive Metastore. Trino 483 bundles Hadoop 3.3, so ofs locations
 * are read with the hadoop2 Ozone File System library added to the Iceberg plugin.
 */
public final class TrinoEngine implements Startable, QueryEngine {
    public static final String CATALOG = "iceberg";

    private static final Logger LOGGER = LoggerFactory.getLogger(TrinoEngine.class);

    private static final String IMAGE_PROPERTY = "trino.image";

    private static final String IMAGE_DEFAULT = "trinodb/trino:483";

    private static final String HEAP = "1G";

    private static final String OZONE_FILE_SYSTEM_LIBRARY_PROPERTY = "trino.ozone.filesystem.library";

    private static final String OZONE_FILE_SYSTEM_LIBRARY = "ozone-filesystem-hadoop2.jar";

    private static final String OZONE_FILE_SYSTEM_LIBRARY_DEFAULT = "target/trino-ozone/" + OZONE_FILE_SYSTEM_LIBRARY;

    private static final String HDFS_LIBRARY_DIRECTORY = "/usr/lib/trino/plugin/iceberg/hdfs";

    private static final String CONFIGURATION_DIRECTORY = "/etc/trino";

    private static final String JVM_CONFIG_PATH = CONFIGURATION_DIRECTORY + "/jvm.config";

    private static final String CATALOG_PATH = "%s/catalog/%s.properties".formatted(CONFIGURATION_DIRECTORY, CATALOG);

    private static final String CLIENT_SITE_PATH = CONFIGURATION_DIRECTORY + "/client-site.xml";

    private static final String JVM_CONFIG_RESOURCE = "trino/jvm.config";

    private static final String CATALOG_RESOURCE = "trino/iceberg.properties";

    private static final int READABLE_FILE_MODE = Integer.parseInt("644", 8);

    private static final String STARTED_PATTERN = ".*SERVER STARTED.*\\n";

    private static final Duration CATALOG_TIMEOUT = Duration.ofMinutes(3);

    private static final String CLI = "trino";

    private static final String SERVER_ARGUMENT = "--server";

    private static final String SERVER = "http://%s:%d".formatted(ClusterContext.HOST, ClusterContext.TRINO_PORT);

    private static final String USER_ARGUMENT = "--user";

    private static final String USER = "trino";

    private static final String OUTPUT_FORMAT_ARGUMENT = "--output-format";

    private static final String OUTPUT_FORMAT = "TSV";

    private static final String SESSION_ARGUMENT = "--session";

    private static final String MAX_RUN_TIME_SESSION = "query_max_run_time=5m";

    private static final String EXECUTE_ARGUMENT = "--execute";

    private static final String DEBUG_ARGUMENT = "--debug";

    private static final String SHOW_SCHEMAS = "SHOW SCHEMAS FROM " + CATALOG;

    private static final String DEFAULT_SCHEMA = "default";

    private static final String LOG_PREFIX = "trino";

    private static final int LOG_TAIL_LINES = 50;

    private final GenericContainer<?> container;

    /**
     * Trino engine with an Iceberg catalog on the cluster Hive Metastore
     *
     * @param context Cluster context with the network namespace, krb5.conf and keytab directory
     * @param clientSite Host path of the merged client-site.xml with core site, HDFS, Ozone and Metastore client properties
     */
    public TrinoEngine(final ClusterContext context, final Path clientSite) {
        final Map<String, String> jvmVariables = new LinkedHashMap<>();
        jvmVariables.put("heap", HEAP);
        jvmVariables.put("krb5Conf", ClusterContext.KRB5_CONF_PATH);

        final String keytab = context.containerKeytab(ClusterContext.TRINO_KEYTAB);
        final Map<String, String> catalogVariables = new LinkedHashMap<>();
        catalogVariables.put("metastoreUri", MetastoreComponent.METASTORE_URI);
        catalogVariables.put("metastorePrincipal", ClusterContext.HIVE_PRINCIPAL);
        catalogVariables.put("principal", ClusterContext.TRINO_PRINCIPAL);
        catalogVariables.put("keytab", keytab);
        catalogVariables.put("configResources", CLIENT_SITE_PATH);

        this.container = new GenericContainer<>(DockerImageName.parse(System.getProperty(IMAGE_PROPERTY, IMAGE_DEFAULT)))
                .withCopyToContainer(Transferable.of(context.krb5Conf()), ClusterContext.KRB5_CONF_PATH)
                .withCopyToContainer(Transferable.of(ClusterFiles.resource(JVM_CONFIG_RESOURCE, jvmVariables)), JVM_CONFIG_PATH)
                .withCopyToContainer(Transferable.of(ClusterFiles.resource(CATALOG_RESOURCE, catalogVariables)), CATALOG_PATH)
                .withCopyToContainer(MountableFile.forHostPath(clientSite, READABLE_FILE_MODE), CLIENT_SITE_PATH)
                .withFileSystemBind(context.keytabDirectory().toString(), ClusterContext.KEYTAB_DIRECTORY, BindMode.READ_ONLY)
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withNetworkMode(context.networkMode()))
                .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix(LOG_PREFIX))
                .waitingFor(Wait.forLogMessage(STARTED_PATTERN, 1).withStartupTimeout(ClusterContext.STARTUP_TIMEOUT));

        final Path ozoneFileSystemLibrary = Path.of(System.getProperty(OZONE_FILE_SYSTEM_LIBRARY_PROPERTY, OZONE_FILE_SYSTEM_LIBRARY_DEFAULT)).toAbsolutePath();
        if (!Files.isRegularFile(ozoneFileSystemLibrary)) {
            throw new IllegalStateException("Ozone File System library [%s] not found: run the Maven build without -DskipTests to copy it".formatted(ozoneFileSystemLibrary));
        }
        container.withCopyToContainer(MountableFile.forHostPath(ozoneFileSystemLibrary, READABLE_FILE_MODE), "%s/%s".formatted(HDFS_LIBRARY_DIRECTORY, OZONE_FILE_SYSTEM_LIBRARY));
    }

    /**
     * Start the Trino server and wait until listing schemas of the Iceberg catalog succeeds
     */
    @Override
    public void start() {
        container.start();
        final AtomicBoolean stopped = new AtomicBoolean();
        try {
            ClusterFiles.waitUntil("Trino Iceberg catalog", CATALOG_TIMEOUT, () -> {
                if (!container.isRunning()) {
                    stopped.set(true);
                    return;
                }
                final List<String> schemas = query(SHOW_SCHEMAS);
                if (!schemas.contains(DEFAULT_SCHEMA)) {
                    throw new IllegalStateException("Schema [%s] not found in %s".formatted(DEFAULT_SCHEMA, schemas));
                }
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Waiting for Trino interrupted", e);
        }

        if (stopped.get()) {
            throw new IllegalStateException("Trino container stopped:%n%s".formatted(logTail()));
        }
    }

    @Override
    public void stop() {
        container.stop();
    }

    /**
     * Run SQL statements with the Trino CLI in the container as user trino
     *
     * @param sql SQL statements separated with semicolons
     * @return Result rows in TSV format without a header line
     * @throws Exception Thrown when the CLI returned a non-zero exit code or the command failed
     */
    @Override
    public List<String> query(final String sql) throws Exception {
        final ExecResult result = execute(sql, List.of());
        if (result.getExitCode() != 0) {
            throw new IllegalStateException("Trino query failed with exit code %d [%s]:%n%s%s".formatted(result.getExitCode(), sql, result.getStderr(), result.getStdout()));
        }
        return result.getStdout().lines().toList();
    }

    /**
     * Run SQL statements expected to fail with the Trino CLI in debug mode, which prints the causes of the failure
     *
     * @param sql SQL statements separated with semicolons
     * @return Result rows printed before the failure and the complete error output
     * @throws Exception Thrown when the CLI returned exit code zero or the command failed
     */
    @Override
    public FailedQuery queryFailure(final String sql) throws Exception {
        final ExecResult result = execute(sql, List.of(DEBUG_ARGUMENT));
        if (result.getExitCode() == 0) {
            throw new IllegalStateException("Trino query expected to fail succeeded [%s]:%n%s".formatted(sql, result.getStdout()));
        }
        return new FailedQuery(result.getStdout().lines().toList(), result.getStderr() + result.getStdout());
    }

    private ExecResult execute(final String sql, final List<String> options) throws IOException, InterruptedException {
        final List<String> command = new ArrayList<>(List.of(CLI,
                SERVER_ARGUMENT, SERVER,
                USER_ARGUMENT, USER,
                OUTPUT_FORMAT_ARGUMENT, OUTPUT_FORMAT,
                SESSION_ARGUMENT, MAX_RUN_TIME_SESSION));
        command.addAll(options);
        command.add(EXECUTE_ARGUMENT);
        command.add(sql);
        return container.execInContainer(command.toArray(String[]::new));
    }

    private String logTail() {
        final List<String> logs = container.getLogs().lines().toList();
        return String.join(System.lineSeparator(), logs.subList(Math.max(0, logs.size() - LOG_TAIL_LINES), logs.size()));
    }
}
