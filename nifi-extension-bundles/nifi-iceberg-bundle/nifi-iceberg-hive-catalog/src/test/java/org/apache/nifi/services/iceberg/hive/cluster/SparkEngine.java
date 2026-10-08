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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Spark SQL CLI in local mode with an Iceberg catalog on the Kerberized Hive Metastore, as in ADH Spark: the patched Hive
 * 2.3 Metastore client calling get_table_req and the Ozone File System library are prepended with --driver-class-path.
 */
public final class SparkEngine implements Startable, QueryEngine {
    public static final String CATALOG = "iceberg";

    private static final String METASTORE_LIBRARY = "hive-metastore.jar";

    private static final String HIVE_COMMON_LIBRARY = "hive-common.jar";

    private static final String HIVE_EXEC_LIBRARY = "hive-exec.jar";

    private static final String HIVE_SERDE_LIBRARY = "hive-serde.jar";

    private static final String ICEBERG_LIBRARY = "iceberg-spark-runtime.jar";

    private static final String OZONE_LIBRARY = "ozone-filesystem-hadoop3-client.jar";

    private static final List<String> DRIVER_LIBRARIES = List.of(METASTORE_LIBRARY, HIVE_COMMON_LIBRARY, HIVE_EXEC_LIBRARY, HIVE_SERDE_LIBRARY, OZONE_LIBRARY);

    private static final List<String> LIBRARIES = Stream.concat(DRIVER_LIBRARIES.stream(), Stream.of(ICEBERG_LIBRARY)).toList();

    private static final String CLASS_PATH_SEPARATOR = ":";

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkEngine.class);

    private static final String IMAGE_PROPERTY = "spark.image";

    private static final String IMAGE_DEFAULT = "apache/spark:4.0.1-scala2.13-java21-ubuntu";

    private static final String DRIVER_MEMORY = "1g";

    private static final String LIBRARY_DIRECTORY_PROPERTY = "spark.libraries.directory";

    private static final String LIBRARY_DIRECTORY_DEFAULT = "target/spark-jars";

    private static final String CLUSTER_DIRECTORY = "/opt/spark-cluster";

    private static final String CONFIGURATION_DIRECTORY = CLUSTER_DIRECTORY + "/conf";

    private static final String CONTAINER_LIBRARY_DIRECTORY = CLUSTER_DIRECTORY + "/jars";

    private static final String CONTAINER_FILE_FORMAT = "%s/%s";

    private static final String SPARK_DEFAULTS_RESOURCE = "spark/spark-defaults.conf";

    private static final String LOG4J_RESOURCE = "spark/log4j2.properties";

    private static final String SPARK_DEFAULTS_FILE = "spark-defaults.conf";

    private static final String LOG4J_FILE = "log4j2.properties";

    private static final List<String> CLIENT_SITE_FILES = List.of("core-site.xml", "hive-site.xml");

    private static final String SPARK_CONF_DIR_VARIABLE = "SPARK_CONF_DIR";

    private static final String HADOOP_CONF_DIR_VARIABLE = "HADOOP_CONF_DIR";

    private static final int READABLE_FILE_MODE = Integer.parseInt("644", 8);

    private static final String TINI = "/usr/bin/tini";

    private static final String TINI_SUBREAPER_ARGUMENT = "-s";

    private static final String END_OF_OPTIONS = "--";

    private static final String SLEEP = "sleep";

    private static final String INFINITY = "infinity";

    private static final String READY_COMMAND_FORMAT = "test -r %s && test -r %s";

    private static final Duration CATALOG_TIMEOUT = Duration.ofMinutes(5);

    private static final String TIMEOUT = "timeout";

    private static final String KILL_AFTER_ARGUMENT = "--kill-after=30s";

    private static final String QUERY_TIMEOUT = "600s";

    private static final int TIMEOUT_EXIT_CODE = 124;

    private static final String SPARK_SQL = "/opt/spark/bin/spark-sql";

    private static final String MASTER_ARGUMENT = "--master";

    private static final String MASTER = "local[1]";

    private static final String DRIVER_MEMORY_ARGUMENT = "--driver-memory";

    private static final String DRIVER_CLASS_PATH_ARGUMENT = "--driver-class-path";

    private static final String JARS_ARGUMENT = "--jars";

    private static final String PRINCIPAL_ARGUMENT = "--principal";

    private static final String KEYTAB_ARGUMENT = "--keytab";

    private static final String SILENT_ARGUMENT = "-S";

    private static final String HIVE_CONF_ARGUMENT = "--hiveconf";

    private static final String LOCAL_SCRATCH_DIRECTORY = "hive.exec.scratchdir=file:///tmp/hive-scratch";

    private static final String EXECUTE_ARGUMENT = "-e";

    private static final String SHOW_NAMESPACES = "SHOW NAMESPACES IN " + CATALOG;

    private static final String DEFAULT_NAMESPACE = "default";

    private static final String LOG_PREFIX = "spark";

    private static final int ERROR_TAIL_LINES = 80;

    private final Path libraryDirectory;

    private final String keytab;

    private final GenericContainer<?> container;

    /**
     * Spark engine with an Iceberg catalog on the cluster Hive Metastore
     *
     * @param context Cluster context with the network namespace, krb5.conf and keytab directory
     * @param clientSite Host path of the merged client-site.xml with core site, HDFS, Ozone and Metastore client properties
     */
    public SparkEngine(final ClusterContext context, final Path clientSite) {
        this.libraryDirectory = libraryDirectory();
        this.keytab = context.containerKeytab(ClusterContext.SPARK_KEYTAB);

        final Map<String, String> defaultsVariables = new LinkedHashMap<>();
        defaultsVariables.put("catalog", CATALOG);
        defaultsVariables.put("metastoreUri", MetastoreComponent.METASTORE_URI);
        defaultsVariables.put("metastorePrincipal", ClusterContext.HIVE_PRINCIPAL);
        defaultsVariables.put("warehouse", MetastoreComponent.WAREHOUSE);
        defaultsVariables.put("krb5Conf", ClusterContext.KRB5_CONF_PATH);

        final String readyCommand = READY_COMMAND_FORMAT.formatted(keytab, containerLibrary(OZONE_LIBRARY));

        this.container = new GenericContainer<>(DockerImageName.parse(System.getProperty(IMAGE_PROPERTY, IMAGE_DEFAULT)))
                .withCommand(TINI, TINI_SUBREAPER_ARGUMENT, END_OF_OPTIONS, SLEEP, INFINITY)
                .withEnv(SPARK_CONF_DIR_VARIABLE, CONFIGURATION_DIRECTORY)
                .withEnv(HADOOP_CONF_DIR_VARIABLE, CONFIGURATION_DIRECTORY)
                .withCopyToContainer(Transferable.of(context.krb5Conf()), ClusterContext.KRB5_CONF_PATH)
                .withCopyToContainer(Transferable.of(ClusterFiles.resource(SPARK_DEFAULTS_RESOURCE, defaultsVariables)), configurationFile(SPARK_DEFAULTS_FILE))
                .withCopyToContainer(Transferable.of(ClusterFiles.resource(LOG4J_RESOURCE, Map.of())), configurationFile(LOG4J_FILE))
                .withFileSystemBind(context.keytabDirectory().toString(), ClusterContext.KEYTAB_DIRECTORY, BindMode.READ_ONLY)
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withNetworkMode(context.networkMode()))
                .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix(LOG_PREFIX))
                .waitingFor(Wait.forSuccessfulCommand(readyCommand).withStartupTimeout(ClusterContext.STARTUP_TIMEOUT));

        for (final String clientSiteFile : CLIENT_SITE_FILES) {
            container.withCopyToContainer(MountableFile.forHostPath(clientSite, READABLE_FILE_MODE), configurationFile(clientSiteFile));
        }
        for (final String library : LIBRARIES) {
            container.withCopyToContainer(MountableFile.forHostPath(libraryDirectory.resolve(library), READABLE_FILE_MODE), containerLibrary(library));
        }
    }

    /**
     * Start the container and wait until listing namespaces of the Iceberg catalog succeeds
     */
    @Override
    public void start() {
        for (final String library : LIBRARIES) {
            final Path path = libraryDirectory.resolve(library);
            if (!Files.isRegularFile(path)) {
                throw new IllegalStateException("Spark library [%s] not found: run the Maven build without -DskipTests to copy the Spark libraries".formatted(path));
            }
        }

        container.start();
        try {
            ClusterFiles.waitUntil("Spark Iceberg catalog", CATALOG_TIMEOUT, () -> {
                final List<String> namespaces = query(SHOW_NAMESPACES);
                if (!namespaces.contains(DEFAULT_NAMESPACE)) {
                    throw new IllegalStateException("Namespace [%s] not found in %s".formatted(DEFAULT_NAMESPACE, namespaces));
                }
            });
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Waiting for Spark interrupted", e);
        }
    }

    @Override
    public void stop() {
        container.stop();
    }

    /**
     * Run SQL statements with the Spark SQL CLI in local mode as the spark service principal. Each call starts a new
     * driver JVM, so related statements should be combined in one call.
     *
     * @param sql SQL statements separated with semicolons
     * @return Result rows with tab separated column values and without a header line
     * @throws Exception Thrown when the CLI returned a non-zero exit code, timed out or the command failed
     */
    @Override
    public List<String> query(final String sql) throws Exception {
        final ExecResult result = execute(sql);
        final int exitCode = result.getExitCode();
        if (exitCode == TIMEOUT_EXIT_CODE) {
            throw new IllegalStateException("Spark query timed out after %s [%s]:%n%s".formatted(QUERY_TIMEOUT, sql, tail(result.getStderr())));
        }
        if (exitCode != 0) {
            throw new IllegalStateException("Spark query failed with exit code %d [%s]:%n%s%n%s".formatted(exitCode, sql, tail(result.getStderr()), result.getStdout()));
        }
        return result.getStdout().lines().toList();
    }

    /**
     * Run SQL statements expected to fail with the Spark SQL CLI as the spark service principal. The CLI stops at the
     * first failed statement, so successful statements can precede the failing one in the same driver JVM.
     *
     * @param sql SQL statements separated with semicolons
     * @return Result rows printed before the failure and the complete standard error followed by standard output
     * @throws Exception Thrown when the CLI returned exit code zero, timed out or the command failed
     */
    @Override
    public FailedQuery queryFailure(final String sql) throws Exception {
        final ExecResult result = execute(sql);
        final int exitCode = result.getExitCode();
        if (exitCode == 0 || exitCode == TIMEOUT_EXIT_CODE) {
            throw new IllegalStateException("Spark query expected to fail returned exit code %d [%s]:%n%s%n%s".formatted(exitCode, sql, tail(result.getStderr()), result.getStdout()));
        }
        return new FailedQuery(result.getStdout().lines().toList(), result.getStderr() + result.getStdout());
    }

    private ExecResult execute(final String sql) throws IOException, InterruptedException {
        return container.execInContainer(TIMEOUT, KILL_AFTER_ARGUMENT, QUERY_TIMEOUT,
                SPARK_SQL,
                MASTER_ARGUMENT, MASTER,
                DRIVER_MEMORY_ARGUMENT, DRIVER_MEMORY,
                DRIVER_CLASS_PATH_ARGUMENT, DRIVER_LIBRARIES.stream().map(SparkEngine::containerLibrary).collect(Collectors.joining(CLASS_PATH_SEPARATOR)),
                JARS_ARGUMENT, containerLibrary(ICEBERG_LIBRARY),
                PRINCIPAL_ARGUMENT, ClusterContext.SPARK_PRINCIPAL,
                KEYTAB_ARGUMENT, keytab,
                SILENT_ARGUMENT,
                HIVE_CONF_ARGUMENT, LOCAL_SCRATCH_DIRECTORY,
                EXECUTE_ARGUMENT, sql);
    }

    private static Path libraryDirectory() {
        return Path.of(System.getProperty(LIBRARY_DIRECTORY_PROPERTY, LIBRARY_DIRECTORY_DEFAULT)).toAbsolutePath();
    }

    private static String configurationFile(final String fileName) {
        return CONTAINER_FILE_FORMAT.formatted(CONFIGURATION_DIRECTORY, fileName);
    }

    private static String containerLibrary(final String library) {
        return CONTAINER_FILE_FORMAT.formatted(CONTAINER_LIBRARY_DIRECTORY, library);
    }

    private static String tail(final String output) {
        final List<String> lines = output.lines().toList();
        return String.join(System.lineSeparator(), lines.subList(Math.max(0, lines.size() - ERROR_TAIL_LINES), lines.size()));
    }
}
