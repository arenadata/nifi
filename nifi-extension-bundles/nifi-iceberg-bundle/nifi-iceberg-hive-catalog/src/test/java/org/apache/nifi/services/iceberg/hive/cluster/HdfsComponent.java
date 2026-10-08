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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Kerberized HDFS with one NameNode and one DataNode registered as localhost. The NameNode runs with the Ranger HDFS
 * plugin as inode attributes provider and writes Ranger audit events as JSON lines to a local directory.
 */
public final class HdfsComponent implements Startable {
    private static final String IMAGE_PROPERTY = "hadoop.image";

    private static final String IMAGE_DEFAULT = "apache/hadoop:3.4.1";

    private static final String RANGER_IMAGE_NAME = "nifi-iceberg-hdfs-ranger:1";

    private static final String DOCKERFILE = "Dockerfile";

    private static final String DOCKERFILE_RESOURCE = "cluster/hdfs/Dockerfile";

    private static final String IMAGE_ARGUMENT = "HADOOP_IMAGE";

    private static final String PLUGIN_ARCHIVE = "ranger-hdfs-plugin.tar.gz";

    private static final String PLUGIN_DIRECTORY_PROPERTY = "ranger.plugin.directory";

    private static final String PLUGIN_DIRECTORY_DEFAULT = "target/ranger-plugins";

    private static final String ARCHIVE_URL_PREFIX = "https://archive.apache.org/dist/";

    private static final String MIRROR_URL_PREFIX = "https://dlcdn.apache.org/";

    private static final String CHECKSUM_EXTENSION = ".sha512";

    private static final String CHECKSUM_ALGORITHM = "SHA-512";

    private static final String PARTIAL_EXTENSION = ".partial";

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);

    private static final Duration DOWNLOAD_TIMEOUT = Duration.ofMinutes(20);

    private static final int HTTP_OK = 200;

    private static final int CHECKSUM_LENGTH = 128;

    private static final String SERVICE_TYPE = "hdfs";

    private static final String CONFIGURATION_DIRECTORY = "/opt/hadoop/etc/hadoop";

    private static final String CONFIGURATION_FILE_FORMAT = CONFIGURATION_DIRECTORY + "/%s";

    private static final String CORE_SITE = "core-site.xml";

    private static final String HDFS_SITE = "hdfs-site.xml";

    private static final String RANGER_FILE_PREFIX = "ranger-";

    private static final String XML_EXTENSION = ".xml";

    private static final String ROOT_PATH = "/";

    private static final String SERVICE_AUDIT_FILE_FORMAT = "ranger-hdfs-%s-audit.xml";

    private static final String AUDIT_DIRECTORY = "/tmp/ranger-audit/hdfs";

    private static final String AUDIT_BATCH_INTERVAL = "1000";

    private static final String RANGER_AUTHORIZER = "org.apache.ranger.authorization.hadoop.RangerHdfsAuthorizer";

    private static final String NAME_DIRECTORY_VARIABLE = "ENSURE_NAMENODE_DIR";

    private static final String NAME_DIRECTORY = "/tmp/hadoop-hadoop/dfs/name";

    private static final String HEAP_VARIABLE = "HADOOP_HEAPSIZE_MAX";

    private static final String NAME_NODE_HEAP = "768m";

    private static final String DATA_NODE_HEAP = "512m";

    private static final String HDFS_COMMAND = "hdfs";

    private static final String NAME_NODE_COMMAND = "namenode";

    private static final String DATA_NODE_COMMAND = "datanode";

    private static final String NAME_NODE_STARTED_PATTERN = ".*NameNode RPC up at.*";

    private static final String DATA_NODE_REGISTERED_PATTERN = ".*successfully registered with NN.*";

    private static final String SHELL = "/bin/bash";

    private static final String SHELL_COMMAND_OPTION = "-c";

    private static final String RPC_TIMEOUT_OPTION = "ipc.client.rpc-timeout.ms=10000";

    private static final String READINESS_COMMAND_FORMAT = "kinit -k -t %1$s %2$s && hdfs dfsadmin -D %3$s -report -live && hdfs dfsadmin -D %3$s -safemode get";

    private static final String LIVE_DATA_NODES = "Live datanodes (1)";

    private static final String SAFE_MODE_OFF = "Safe mode is OFF";

    private static final String AUDIT_COMMAND_FORMAT = "if [ -d %1$s ]; then find %1$s -type f | sort | while read -r file; do cat \"${file}\"; done; fi";

    private static final String FILE_SYSTEM_URI = "hdfs://%s:%d".formatted(ClusterContext.HOST, ClusterContext.NAME_NODE_PORT);

    private static final String REPLICATION_PROPERTY = "dfs.replication";

    private static final String DATA_TRANSFER_PROTECTION_PROPERTY = "dfs.data.transfer.protection";

    private static final String NAME_NODE_PRINCIPAL_PROPERTY = "dfs.namenode.kerberos.principal";

    private static final String USE_DATA_NODE_HOSTNAME_PROPERTY = "dfs.client.use.datanode.hostname";

    private static final String ENABLED = "true";

    private static final String ALL = "*";

    private static final String PATTERN_SUFFIX = ".pattern";

    private static final String BIND_ALL = "0.0.0.0";

    private static final String REPLICATION = "1";

    private static final String DATA_TRANSFER_PROTECTION = "authentication";

    private static final String NAME_NODE_PREFIX = "namenode";

    private static final String DATA_NODE_PREFIX = "datanode";

    private final ClusterContext context;

    private final GenericContainer<?> nameNode;

    private final GenericContainer<?> dataNode;

    public HdfsComponent(final ClusterContext context, final RangerComponent ranger) {
        this.context = context;
        final String serviceName = Objects.requireNonNull(ranger, "Ranger component required").hdfsServiceName();

        final String image = System.getProperty(IMAGE_PROPERTY, IMAGE_DEFAULT);
        final ImageFromDockerfile rangerImage = new ImageFromDockerfile(RANGER_IMAGE_NAME, false)
                .withFileFromClasspath(DOCKERFILE, DOCKERFILE_RESOURCE)
                .withFileFromPath(PLUGIN_ARCHIVE, downloadPluginArchive())
                .withBuildArg(IMAGE_ARGUMENT, image);
        final GenericContainer<?> nameNodeContainer = new GenericContainer<>(rangerImage);
        ranger.pluginFiles(SERVICE_TYPE, serviceName).forEach((path, content) ->
                nameNodeContainer.withCopyToContainer(Transferable.of(content), getPluginFilePath(path)));
        nameNodeContainer.withCopyToContainer(Transferable.of(ClusterFiles.hadoopXml(getAuditProperties())),
                CONFIGURATION_FILE_FORMAT.formatted(SERVICE_AUDIT_FILE_FORMAT.formatted(serviceName)));
        this.nameNode = configure(nameNodeContainer, ClusterContext.NAME_NODE_KEYTAB, NAME_NODE_PREFIX)
                .withEnv(NAME_DIRECTORY_VARIABLE, NAME_DIRECTORY)
                .withEnv(HEAP_VARIABLE, NAME_NODE_HEAP)
                .withCommand(HDFS_COMMAND, NAME_NODE_COMMAND)
                .waitingFor(Wait.forLogMessage(NAME_NODE_STARTED_PATTERN, 1).withStartupTimeout(ClusterContext.STARTUP_TIMEOUT));

        this.dataNode = configure(new GenericContainer<>(DockerImageName.parse(image)), ClusterContext.DATA_NODE_KEYTAB, DATA_NODE_PREFIX)
                .withEnv(HEAP_VARIABLE, DATA_NODE_HEAP)
                .withCommand(HDFS_COMMAND, DATA_NODE_COMMAND)
                .waitingFor(Wait.forLogMessage(DATA_NODE_REGISTERED_PATTERN, 1).withStartupTimeout(ClusterContext.STARTUP_TIMEOUT))
                .dependsOn(nameNode);
    }

    /**
     * Containers of the NameNode and the DataNode, which depends on the NameNode
     *
     * @return NameNode and DataNode containers
     */
    public List<GenericContainer<?>> containers() {
        return List.of(nameNode, dataNode);
    }

    /**
     * Start the NameNode and the DataNode and wait until the NameNode reports one live DataNode and left safe mode
     */
    @Override
    public void start() {
        nameNode.start();
        dataNode.start();
        try {
            awaitLiveDataNode();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Waiting for HDFS interrupted", e);
        }
    }

    @Override
    public void stop() {
        dataNode.stop();
        nameNode.stop();
    }

    /**
     * HDFS client properties to be combined with the shared core site properties
     *
     * @return HDFS client properties
     */
    public static Map<String, String> clientProperties() {
        final Map<String, String> properties = new LinkedHashMap<>();
        properties.put(USE_DATA_NODE_HOSTNAME_PROPERTY, ENABLED);
        properties.put(DATA_TRANSFER_PROTECTION_PROPERTY, DATA_TRANSFER_PROTECTION);
        properties.put(NAME_NODE_PRINCIPAL_PROPERTY, ClusterContext.NAME_NODE_PRINCIPAL);
        properties.put(NAME_NODE_PRINCIPAL_PROPERTY + PATTERN_SUFFIX, ALL);
        properties.put(REPLICATION_PROPERTY, REPLICATION);
        return Collections.unmodifiableMap(properties);
    }

    /**
     * Create a directory with owner and permission as the admin principal, which is the HDFS superuser
     *
     * @param path Absolute directory path
     * @param owner Owner user name
     * @param permission Octal permission such as 777 or 1777
     * @throws Exception Thrown on login or file system failures
     */
    public void createDirectory(final String path, final String owner, final String permission) throws Exception {
        final Configuration configuration = new Configuration();
        context.coreSite(clientProperties()).forEach(configuration::set);
        UserGroupInformation.setConfiguration(configuration);
        final UserGroupInformation admin = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                ClusterContext.ADMIN_PRINCIPAL, context.keytab(ClusterContext.ADMIN_KEYTAB).toString());
        admin.doAs((PrivilegedExceptionAction<Void>) () -> {
            try (FileSystem fileSystem = FileSystem.newInstance(URI.create(FILE_SYSTEM_URI), configuration)) {
                final org.apache.hadoop.fs.Path directory = new org.apache.hadoop.fs.Path(path);
                if (!fileSystem.mkdirs(directory)) {
                    throw new IOException("Directory [%s] not created".formatted(path));
                }
                fileSystem.setOwner(directory, owner, null);
                fileSystem.setPermission(directory, new FsPermission(permission));
            }
            return null;
        });
    }

    /**
     * Ranger HDFS audit events written by the NameNode as JSON lines
     *
     * @return Audit events in file name order or empty list when no events were written yet
     * @throws Exception Thrown when reading from the NameNode container failed
     */
    public List<String> auditLines() throws Exception {
        final ExecResult result = nameNode.execInContainer(SHELL, SHELL_COMMAND_OPTION, AUDIT_COMMAND_FORMAT.formatted(AUDIT_DIRECTORY));
        if (result.getExitCode() != 0) {
            throw new IllegalStateException("Reading Ranger audit failed with exit code [%d]: %s".formatted(result.getExitCode(), result.getStderr()));
        }
        return result.getStdout().lines().filter(line -> !line.isBlank()).toList();
    }

    private GenericContainer<?> configure(final GenericContainer<?> container, final String keytab, final String prefix) {
        return container
                .withCopyToContainer(Transferable.of(context.krb5Conf()), ClusterContext.KRB5_CONF_PATH)
                .withCopyToContainer(Transferable.of(ClusterFiles.hadoopXml(context.coreSite(Map.of()))), CONFIGURATION_FILE_FORMAT.formatted(CORE_SITE))
                .withCopyToContainer(Transferable.of(ClusterFiles.hadoopXml(getServerProperties(keytab))), CONFIGURATION_FILE_FORMAT.formatted(HDFS_SITE))
                .withFileSystemBind(context.keytabDirectory().toString(), ClusterContext.KEYTAB_DIRECTORY, BindMode.READ_ONLY)
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withNetworkMode(context.networkMode()))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(HdfsComponent.class)).withPrefix(prefix));
    }

    private Map<String, String> getServerProperties(final String keytab) {
        final Map<String, String> properties = new LinkedHashMap<>();
        properties.put(REPLICATION_PROPERTY, REPLICATION);
        properties.put("dfs.permissions.enabled", ENABLED);
        properties.put("dfs.block.access.token.enable", ENABLED);
        properties.put("ignore.secure.ports.for.testing", ENABLED);
        properties.put(DATA_TRANSFER_PROTECTION_PROPERTY, DATA_TRANSFER_PROTECTION);
        properties.put("dfs.namenode.rpc-bind-host", BIND_ALL);
        properties.put(NAME_NODE_PRINCIPAL_PROPERTY, ClusterContext.NAME_NODE_PRINCIPAL);
        properties.put("dfs.namenode.keytab.file", context.containerKeytab(ClusterContext.NAME_NODE_KEYTAB));
        properties.put("dfs.namenode.kerberos.internal.spnego.principal", ClusterContext.HTTP_PRINCIPAL);
        properties.put("dfs.datanode.kerberos.principal", ClusterContext.DATA_NODE_PRINCIPAL);
        properties.put("dfs.datanode.keytab.file", context.containerKeytab(ClusterContext.DATA_NODE_KEYTAB));
        properties.put("dfs.datanode.hostname", ClusterContext.HOST);
        properties.put(USE_DATA_NODE_HOSTNAME_PROPERTY, ENABLED);
        properties.put("dfs.web.authentication.kerberos.principal", ClusterContext.HTTP_PRINCIPAL);
        properties.put("dfs.web.authentication.kerberos.keytab", context.containerKeytab(keytab));
        properties.put("dfs.namenode.inode.attributes.provider.class", RANGER_AUTHORIZER);
        properties.put("dfs.permissions.ContentSummary.subAccess", ENABLED);
        return properties;
    }

    private static Map<String, String> getAuditProperties() {
        final Map<String, String> properties = new LinkedHashMap<>();
        properties.put("xasecure.audit.is.enabled", ENABLED);
        properties.put("xasecure.audit.destination.file", ENABLED);
        properties.put("xasecure.audit.destination.file.dir", AUDIT_DIRECTORY);
        properties.put("xasecure.audit.destination.file.batch.batch.interval.ms", AUDIT_BATCH_INTERVAL);
        return properties;
    }

    private static String getPluginFilePath(final String path) {
        final String fileName = Path.of(path).getFileName().toString();
        final String containerPath;
        if (fileName.startsWith(RANGER_FILE_PREFIX) && fileName.endsWith(XML_EXTENSION)) {
            containerPath = CONFIGURATION_FILE_FORMAT.formatted(fileName);
        } else if (path.startsWith(ROOT_PATH)) {
            containerPath = path;
        } else {
            containerPath = CONFIGURATION_FILE_FORMAT.formatted(path);
        }
        return containerPath;
    }

    private void awaitLiveDataNode() throws InterruptedException {
        final String command = READINESS_COMMAND_FORMAT.formatted(
                context.containerKeytab(ClusterContext.NAME_NODE_KEYTAB), ClusterContext.NAME_NODE_PRINCIPAL, RPC_TIMEOUT_OPTION);
        final long deadline = System.nanoTime() + ClusterContext.STARTUP_TIMEOUT.toNanos();
        String status = "";
        while (System.nanoTime() < deadline) {
            if (!nameNode.isRunning() || !dataNode.isRunning()) {
                throw new IllegalStateException("HDFS container stopped: NameNode running [%s] DataNode running [%s]".formatted(nameNode.isRunning(), dataNode.isRunning()));
            }
            try {
                final ExecResult result = nameNode.execInContainer(SHELL, SHELL_COMMAND_OPTION, command);
                status = result.getStdout() + result.getStderr();
                if (result.getExitCode() == 0 && status.contains(LIVE_DATA_NODES) && status.contains(SAFE_MODE_OFF)) {
                    return;
                }
            } catch (final IOException e) {
                status = e.toString();
            }
            Thread.sleep(ClusterContext.POLL_INTERVAL.toMillis());
        }
        throw new IllegalStateException("HDFS not ready after %s: %s".formatted(ClusterContext.STARTUP_TIMEOUT, status));
    }

    /**
     * Download the Ranger HDFS plugin archive once into a cache directory and verify its SHA-512 checksum
     *
     * @return Plugin archive path
     */
    private static Path downloadPluginArchive() {
        final String archiveUrl = RangerComponent.pluginArchiveUrl(SERVICE_TYPE);
        final String fileName = archiveUrl.substring(archiveUrl.lastIndexOf(ROOT_PATH) + 1);
        final Path directory = Path.of(System.getProperty(PLUGIN_DIRECTORY_PROPERTY, PLUGIN_DIRECTORY_DEFAULT)).toAbsolutePath();
        final Path archive = directory.resolve(fileName);
        if (Files.isRegularFile(archive)) {
            return archive;
        }

        final Set<String> urls = new LinkedHashSet<>(List.of(archiveUrl.replace(ARCHIVE_URL_PREFIX, MIRROR_URL_PREFIX), archiveUrl));
        final List<String> failures = new ArrayList<>();
        try (HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).connectTimeout(CONNECT_TIMEOUT).build()) {
            Files.createDirectories(directory);
            final Path partial = directory.resolve(fileName + PARTIAL_EXTENSION);
            for (final String url : urls) {
                try {
                    final String expectedChecksum = getChecksum(client, url + CHECKSUM_EXTENSION, fileName);
                    final HttpResponse<Path> response = client.send(HttpRequest.newBuilder(URI.create(url)).timeout(DOWNLOAD_TIMEOUT).build(),
                            HttpResponse.BodyHandlers.ofFile(partial));
                    if (response.statusCode() != HTTP_OK) {
                        throw new IOException("HTTP status [%d]".formatted(response.statusCode()));
                    }
                    final String checksum = getChecksum(partial);
                    if (!checksum.equals(expectedChecksum)) {
                        throw new IOException("Checksum [%s] not matched expected [%s]".formatted(checksum, expectedChecksum));
                    }
                    Files.move(partial, archive, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                    return archive;
                } catch (final IOException e) {
                    failures.add("%s: %s".formatted(url, e));
                    Files.deleteIfExists(partial);
                }
            }
        } catch (final IOException e) {
            throw new UncheckedIOException("Ranger HDFS plugin download to [%s] failed".formatted(directory), e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Ranger HDFS plugin download interrupted", e);
        }
        throw new IllegalStateException("Ranger HDFS plugin download failed %s".formatted(failures));
    }

    private static String getChecksum(final HttpClient client, final String url, final String fileName) throws IOException, InterruptedException {
        final HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create(url)).timeout(CONNECT_TIMEOUT).build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != HTTP_OK) {
            throw new IOException("Checksum HTTP status [%d]".formatted(response.statusCode()));
        }
        final String checksum = response.body().replace(fileName, "").replace(":", "").replaceAll("\\s+", "").toLowerCase(Locale.ROOT);
        if (checksum.length() != CHECKSUM_LENGTH) {
            throw new IOException("Checksum format not supported [%s]".formatted(response.body()));
        }
        return checksum;
    }

    private static String getChecksum(final Path file) throws IOException {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(CHECKSUM_ALGORITHM);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("Digest algorithm [%s] not available".formatted(CHECKSUM_ALGORITHM), e);
        }
        try (InputStream inputStream = new DigestInputStream(Files.newInputStream(file), digest)) {
            inputStream.transferTo(OutputStream.nullOutputStream());
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
