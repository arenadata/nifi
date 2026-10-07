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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
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

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Kerberized Apache Ozone with Storage Container Manager, Ozone Manager and DataNode in one container. The Ozone Manager
 * uses the Ranger Ozone plugin when configured, otherwise the native Ozone ACL authorizer.
 */
public final class OzoneComponent implements Startable {
    private static final String IMAGE_PROPERTY = "ozone.image";

    private static final String IMAGE_DEFAULT = "apache/ozone:2.1.2-slim";

    private static final String IMAGE_NAME_FORMAT = "nifi-iceberg-ozone:%s";

    private static final String RANGER_IMAGE_TAG_SUFFIX = "-ranger";

    private static final String IMAGE_TAG_INVALID_CHARACTERS = "[^a-z0-9_.-]";

    private static final String IMAGE_TAG_SEPARATOR = "-";

    private static final String DOCKERFILE = "Dockerfile";

    private static final String DOCKERFILE_RESOURCE = "cluster/ozone/Dockerfile";

    private static final String START_SCRIPT_RESOURCE = "ozone/start.sh";

    private static final String IMAGE_ARGUMENT = "OZONE_IMAGE";

    private static final String PLUGIN_URLS_ARGUMENT = "RANGER_PLUGIN_URLS";

    private static final String ARCHIVE_URL_PREFIX = "https://archive.apache.org/dist/";

    private static final String MIRROR_URL_PREFIX = "https://dlcdn.apache.org/";

    private static final String URL_SEPARATOR = " ";

    private static final String START_SCRIPT_PATH = "/opt/ozone-cluster/start.sh";

    private static final int EXECUTABLE_MODE = Integer.parseInt("755", 8);

    private static final String SHELL = "/bin/bash";

    private static final String SHELL_COMMAND_OPTION = "-c";

    private static final String CONFIGURATION_DIRECTORY = "/etc/hadoop";

    private static final String CONFIGURATION_FILE_FORMAT = CONFIGURATION_DIRECTORY + "/%s";

    private static final String CORE_SITE = "core-site.xml";

    private static final String OZONE_SITE = "ozone-site.xml";

    private static final String CHECK_PRINCIPAL_VARIABLE = "OZONE_CHECK_PRINCIPAL";

    private static final String CHECK_KEYTAB_VARIABLE = "OZONE_CHECK_KEYTAB";

    private static final String READY_TIMEOUT_VARIABLE = "OZONE_READY_TIMEOUT_SECONDS";

    private static final String READY_MESSAGE = "Ozone cluster ready";

    private static final String FAILED_MESSAGE = "Ozone cluster failed";

    private static final String STARTED_PATTERN = ".*(%s|%s).*".formatted(READY_MESSAGE, FAILED_MESSAGE);

    private static final String LOG_PREFIX = "ozone";

    private static final String SERVICE_TYPE = "ozone";

    private static final String RANGER_FILE_PREFIX = "ranger-";

    private static final String XML_EXTENSION = ".xml";

    private static final String ROOT_PATH = "/";

    private static final String SERVICE_AUDIT_FILE_FORMAT = "ranger-ozone-%s-audit.xml";

    private static final String AUDIT_DIRECTORY = "/tmp/ranger-audit/ozone";

    private static final String AUDIT_BATCH_INTERVAL = "1000";

    private static final String AUDIT_COMMAND_FORMAT = "if [ -d %1$s ]; then find %1$s -type f -exec cat {} +; fi";

    private static final String NATIVE_AUTHORIZER = "org.apache.hadoop.ozone.security.acl.OzoneNativeAuthorizer";

    private static final String RANGER_AUTHORIZER = "org.apache.ranger.authorization.ozone.authorizer.RangerOzoneAuthorizer";

    private static final String ADDRESS_FORMAT = "%s:%d";

    private static final String ALL_INTERFACES = "0.0.0.0";

    private static final String FILE_SYSTEM_URI = "ofs://%s:%d".formatted(ClusterContext.HOST, ClusterContext.OZONE_MANAGER_PORT);

    private static final String LOCATION_FORMAT = "%s/%s/%s";

    private static final String BUCKET_PATH_FORMAT = "/%s/%s";

    private static final String PROBE_FILE = ".cluster-probe";

    private static final byte[] PROBE_CONTENT = "ozone-cluster-probe".getBytes(StandardCharsets.UTF_8);

    private static final String ENABLED = "true";

    private static final String DISABLED = "false";

    private static final String ONE = "1";

    private static final String METADATA_DIRECTORY = "/data/metadata";

    private static final String OZONE_MANAGER_ADDRESS_PROPERTY = "ozone.om.address";

    private static final String ADMINISTRATORS = "hadoop,admin";

    private static final List<String> CLUSTER_USERS = List.of("hive", "nifi");

    private static final Duration BOOTSTRAP_TIMEOUT = Duration.ofMinutes(3);

    private final ClusterContext context;

    private final RangerComponent ranger;

    private final ImageFromDockerfile image;

    private final GenericContainer<?> container;

    private UserGroupInformation adminUser;

    public OzoneComponent(final ClusterContext context, final RangerComponent ranger) {
        this.context = context;
        this.ranger = ranger;

        final String baseImage = System.getProperty(IMAGE_PROPERTY, IMAGE_DEFAULT);
        final String pluginUrls;
        final String imageTag;
        if (ranger == null) {
            pluginUrls = "";
            imageTag = getImageTag(baseImage);
        } else {
            final String archiveUrl = RangerComponent.pluginArchiveUrl(SERVICE_TYPE);
            final Set<String> urls = new LinkedHashSet<>(List.of(archiveUrl.replace(ARCHIVE_URL_PREFIX, MIRROR_URL_PREFIX), archiveUrl));
            pluginUrls = String.join(URL_SEPARATOR, urls);
            imageTag = getImageTag(baseImage) + RANGER_IMAGE_TAG_SUFFIX;
        }
        this.image = new ImageFromDockerfile(IMAGE_NAME_FORMAT.formatted(imageTag), false)
                .withFileFromClasspath(DOCKERFILE, DOCKERFILE_RESOURCE)
                .withBuildArg(IMAGE_ARGUMENT, baseImage)
                .withBuildArg(PLUGIN_URLS_ARGUMENT, pluginUrls);

        this.container = new GenericContainer<>(image)
                .withCopyToContainer(Transferable.of(context.krb5Conf()), ClusterContext.KRB5_CONF_PATH)
                .withCopyToContainer(Transferable.of(ClusterFiles.resource(START_SCRIPT_RESOURCE, Map.of()), EXECUTABLE_MODE), START_SCRIPT_PATH)
                .withCopyToContainer(Transferable.of(ClusterFiles.hadoopXml(context.coreSite(Map.of(OZONE_MANAGER_ADDRESS_PROPERTY, getServerAddress())))),
                        CONFIGURATION_FILE_FORMAT.formatted(CORE_SITE))
                .withCopyToContainer(Transferable.of(ClusterFiles.hadoopXml(getServerProperties(context, ranger != null))), CONFIGURATION_FILE_FORMAT.formatted(OZONE_SITE))
                .withFileSystemBind(context.keytabDirectory().toString(), ClusterContext.KEYTAB_DIRECTORY, BindMode.READ_ONLY)
                .withEnv(CHECK_PRINCIPAL_VARIABLE, ClusterContext.SCM_PRINCIPAL)
                .withEnv(CHECK_KEYTAB_VARIABLE, context.containerKeytab(ClusterContext.SCM_KEYTAB))
                .withEnv(READY_TIMEOUT_VARIABLE, Long.toString(ClusterContext.STARTUP_TIMEOUT.minusMinutes(1).toSeconds()))
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withNetworkMode(context.networkMode()))
                .withCommand(SHELL, START_SCRIPT_PATH)
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(OzoneComponent.class)).withPrefix(LOG_PREFIX))
                .waitingFor(Wait.forLogMessage(STARTED_PATTERN, 1).withStartupTimeout(ClusterContext.STARTUP_TIMEOUT));
    }

    /**
     * Build the Ozone image, including the Ranger plugin when configured, without starting the container
     */
    public void buildImage() {
        image.get();
    }

    /**
     * Start the container and wait until the Storage Container Manager left safe mode with an open RATIS/ONE pipeline
     * and the Ozone Manager accepts Kerberos authenticated client connections from the test process
     */
    @Override
    public void start() {
        if (ranger != null) {
            final String serviceName = ranger.ozoneServiceName();
            ranger.pluginFiles(SERVICE_TYPE, serviceName).forEach((path, content) ->
                    container.withCopyToContainer(Transferable.of(content), getPluginFilePath(path)));
            container.withCopyToContainer(Transferable.of(ClusterFiles.hadoopXml(getAuditProperties())),
                    CONFIGURATION_FILE_FORMAT.formatted(SERVICE_AUDIT_FILE_FORMAT.formatted(serviceName)));
        }
        container.start();
        if (container.getLogs().contains(FAILED_MESSAGE)) {
            throw new IllegalStateException("Ozone start failed, see container log with prefix [%s]".formatted(LOG_PREFIX));
        }
        try {
            ClusterFiles.waitUntil("Ozone Manager", ClusterContext.STARTUP_TIMEOUT, this::connect);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Waiting for Ozone Manager interrupted", e);
        }
    }

    @Override
    public void stop() {
        container.stop();
    }

    /**
     * Ozone client properties to be combined with the shared core site properties
     *
     * @return Ozone client properties
     */
    public static Map<String, String> clientProperties() {
        final Map<String, String> properties = new LinkedHashMap<>();
        properties.put(OZONE_MANAGER_ADDRESS_PROPERTY, ADDRESS_FORMAT.formatted(ClusterContext.HOST, ClusterContext.OZONE_MANAGER_PORT));
        properties.put("ozone.security.enabled", ENABLED);
        properties.put("ozone.om.kerberos.principal", ClusterContext.OZONE_MANAGER_PRINCIPAL);
        properties.put("hdds.scm.kerberos.principal", ClusterContext.SCM_PRINCIPAL);
        properties.put("hdds.datanode.use.datanode.hostname", ENABLED);
        properties.put("hdds.block.token.enabled", ENABLED);
        properties.put("hdds.container.token.enabled", ENABLED);
        properties.put("hdds.grpc.tls.enabled", DISABLED);
        properties.put("ozone.replication", ONE);
        properties.put("ozone.replication.type", "RATIS");
        properties.put("ozone.client.failover.max.attempts", "5");
        properties.put("fs.ofs.impl", "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem");
        return Collections.unmodifiableMap(properties);
    }

    /**
     * Location of a bucket for the ofs file system as used by clients
     *
     * @param volume Volume name
     * @param bucket Bucket name
     * @return Bucket location such as ofs://localhost:9862/volume/bucket
     */
    public static String location(final String volume, final String bucket) {
        return LOCATION_FORMAT.formatted(FILE_SYSTEM_URI, volume, bucket);
    }

    /**
     * Create volume and bucket as the admin principal, verify writing and reading a file, then grant the owner and the
     * cluster users access to the volume and bucket, including default ACLs. Retries until success or timeout.
     *
     * @param volume Volume name
     * @param bucket Bucket name
     * @param owner Owner user name
     * @throws Exception Thrown on login or Ozone failures
     */
    public void createBucket(final String volume, final String bucket, final String owner) throws Exception {
        final Configuration configuration = getClientConfiguration();
        final Path bucketPath = new Path(BUCKET_PATH_FORMAT.formatted(volume, bucket));
        ClusterFiles.waitUntil("Ozone bucket [%s]".formatted(bucketPath), BOOTSTRAP_TIMEOUT, () -> getAdminUser().doAs((PrivilegedExceptionAction<Void>) () -> {
            writeProbe(configuration, bucketPath);
            grantAccess(configuration, volume, bucket, owner);
            return null;
        }));
    }

    /**
     * Ranger Ozone audit events written by the Ozone Manager as JSON lines
     *
     * @return Audit events or empty list when Ranger is not configured or no events were written yet
     * @throws Exception Thrown when reading from the Ozone container failed
     */
    public List<String> auditLines() throws Exception {
        if (ranger == null) {
            return List.of();
        }
        final ExecResult result = container.execInContainer(SHELL, SHELL_COMMAND_OPTION, AUDIT_COMMAND_FORMAT.formatted(AUDIT_DIRECTORY));
        if (result.getExitCode() != 0) {
            throw new IllegalStateException("Reading Ranger audit failed with exit code [%d]: %s".formatted(result.getExitCode(), result.getStderr()));
        }
        return result.getStdout().lines().filter(line -> !line.isBlank()).toList();
    }

    private void connect() throws Exception {
        final OzoneConfiguration configuration = new OzoneConfiguration(getClientConfiguration());
        getAdminUser().doAs((PrivilegedExceptionAction<Void>) () -> {
            try (OzoneClient client = OzoneClientFactory.getRpcClient(configuration)) {
                if (client.getProxy().getOzoneManagerClient().getServiceList().isEmpty()) {
                    throw new IOException("Ozone Manager returned no services");
                }
            }
            return null;
        });
    }

    private static void writeProbe(final Configuration configuration, final Path bucketPath) throws IOException {
        try (FileSystem fileSystem = FileSystem.newInstance(URI.create(FILE_SYSTEM_URI), configuration)) {
            if (!fileSystem.mkdirs(bucketPath)) {
                throw new IOException("Bucket [%s] not created".formatted(bucketPath));
            }
            final Path probe = new Path(bucketPath, PROBE_FILE);
            try (FSDataOutputStream outputStream = fileSystem.create(probe, true)) {
                outputStream.write(PROBE_CONTENT);
            }
            try (FSDataInputStream inputStream = fileSystem.open(probe)) {
                final byte[] content = inputStream.readAllBytes();
                if (!Arrays.equals(PROBE_CONTENT, content)) {
                    throw new IOException("Probe [%s] content not matched".formatted(probe));
                }
            }
            fileSystem.delete(probe, false);
        }
    }

    private static void grantAccess(final Configuration configuration, final String volume, final String bucket, final String owner) throws IOException {
        try (OzoneClient client = OzoneClientFactory.getRpcClient(new OzoneConfiguration(configuration))) {
            final ObjectStore objectStore = client.getObjectStore();
            final OzoneVolume ozoneVolume = objectStore.getVolume(volume);
            ozoneVolume.setOwner(owner);
            ozoneVolume.getBucket(bucket).setOwner(owner);

            final OzoneObj volumeObject = OzoneObjInfo.Builder.newBuilder()
                    .setResType(OzoneObj.ResourceType.VOLUME)
                    .setStoreType(OzoneObj.StoreType.OZONE)
                    .setVolumeName(volume)
                    .build();
            final OzoneObj bucketObject = OzoneObjInfo.Builder.newBuilder()
                    .setResType(OzoneObj.ResourceType.BUCKET)
                    .setStoreType(OzoneObj.StoreType.OZONE)
                    .setVolumeName(volume)
                    .setBucketName(bucket)
                    .build();

            final Set<String> users = new LinkedHashSet<>();
            users.add(owner);
            users.addAll(CLUSTER_USERS);
            for (final String user : users) {
                objectStore.addAcl(volumeObject, OzoneAcl.of(ACLIdentityType.USER, user, OzoneAcl.AclScope.ACCESS, ACLType.READ, ACLType.LIST));
                objectStore.addAcl(bucketObject, OzoneAcl.of(ACLIdentityType.USER, user, OzoneAcl.AclScope.ACCESS, ACLType.ALL));
                objectStore.addAcl(bucketObject, OzoneAcl.of(ACLIdentityType.USER, user, OzoneAcl.AclScope.DEFAULT, ACLType.ALL));
            }
        }
    }

    private synchronized UserGroupInformation getAdminUser() throws IOException {
        if (adminUser == null) {
            final Configuration configuration = getClientConfiguration();
            synchronized (UserGroupInformation.class) {
                UserGroupInformation.setConfiguration(configuration);
                adminUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                        ClusterContext.ADMIN_PRINCIPAL, context.keytab(ClusterContext.ADMIN_KEYTAB).toString());
            }
        } else {
            adminUser.checkTGTAndReloginFromKeytab();
        }
        return adminUser;
    }

    private Configuration getClientConfiguration() {
        final Configuration configuration = new Configuration();
        context.coreSite(clientProperties()).forEach(configuration::set);
        return configuration;
    }

    private static Map<String, String> getServerProperties(final ClusterContext context, final boolean rangerEnabled) {
        final Map<String, String> properties = new LinkedHashMap<>(clientProperties());
        properties.put(OZONE_MANAGER_ADDRESS_PROPERTY, getServerAddress());
        properties.put("ozone.scm.names", ClusterContext.HOST);
        properties.put("ozone.scm.client.address", ClusterContext.HOST);
        properties.put("ozone.scm.block.client.address", ClusterContext.HOST);
        properties.put("ozone.metadata.dirs", METADATA_DIRECTORY);
        properties.put("ozone.scm.datanode.id.dir", METADATA_DIRECTORY);
        properties.put("hdds.datanode.dir", "/data/hdds");
        properties.put("hdds.container.ratis.datanode.storage.dir", METADATA_DIRECTORY + "/dn.ratis");
        properties.put("hdds.datanode.hostname", ClusterContext.HOST);
        properties.put("ozone.server.default.replication", ONE);
        properties.put("ozone.server.default.replication.type", "RATIS");
        properties.put("hdds.scm.safemode.min.datanode", ONE);
        properties.put("ozone.datanode.pipeline.limit", ONE);
        properties.put("ozone.scm.pipeline.owner.container.count", ONE);
        properties.put("ozone.scm.pipeline.creation.interval", "5s");
        properties.put("ozone.scm.container.size", "1GB");
        properties.put("ozone.scm.block.size", "1MB");
        properties.put("ozone.scm.datanode.ratis.volume.free-space.min", "10MB");
        properties.put("hdds.datanode.volume.min.free.space", "100MB");
        properties.put("hdds.datanode.volume.min.free.space.percent", "0");
        properties.put("hdds.scmclient.max.retry.timeout", "30s");
        properties.put("hdds.heartbeat.interval", "5s");
        properties.put("hdds.container.report.interval", "60s");
        properties.put("ozone.scm.stale.node.interval", "30s");
        properties.put("ozone.scm.dead.node.interval", "45s");
        properties.put("hdds.scm.wait.time.after.safemode.exit", "5s");
        properties.put("hdds.container.ratis.datastream.enabled", DISABLED);
        properties.put("ozone.http.basedir", "/tmp/ozone_http");

        properties.put("ozone.security.http.kerberos.enabled", DISABLED);
        properties.put("hdds.scm.kerberos.keytab.file", context.containerKeytab(ClusterContext.SCM_KEYTAB));
        properties.put("ozone.om.kerberos.keytab.file", context.containerKeytab(ClusterContext.OZONE_MANAGER_KEYTAB));
        properties.put("hdds.datanode.kerberos.principal", ClusterContext.OZONE_DATA_NODE_PRINCIPAL);
        properties.put("hdds.datanode.kerberos.keytab.file", context.containerKeytab(ClusterContext.OZONE_DATA_NODE_KEYTAB));
        properties.put("hdds.scm.http.auth.kerberos.principal", ClusterContext.HTTP_PRINCIPAL);
        properties.put("hdds.scm.http.auth.kerberos.keytab", context.containerKeytab(ClusterContext.SCM_KEYTAB));
        properties.put("ozone.om.http.auth.kerberos.principal", ClusterContext.HTTP_PRINCIPAL);
        properties.put("ozone.om.http.auth.kerberos.keytab", context.containerKeytab(ClusterContext.OZONE_MANAGER_KEYTAB));
        properties.put("hdds.datanode.http.auth.kerberos.principal", ClusterContext.HTTP_PRINCIPAL);
        properties.put("hdds.datanode.http.auth.kerberos.keytab", context.containerKeytab(ClusterContext.OZONE_DATA_NODE_KEYTAB));
        properties.put("ozone.administrators", ADMINISTRATORS);
        properties.put("ozone.acl.enabled", ENABLED);
        properties.put("ozone.acl.authorizer.class", rangerEnabled ? RANGER_AUTHORIZER : NATIVE_AUTHORIZER);
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
        final String fileName = java.nio.file.Path.of(path).getFileName().toString();
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

    private static String getServerAddress() {
        return ADDRESS_FORMAT.formatted(ALL_INTERFACES, ClusterContext.OZONE_MANAGER_PORT);
    }

    private static String getImageTag(final String baseImage) {
        return baseImage.toLowerCase(Locale.ROOT).replaceAll(IMAGE_TAG_INVALID_CHARACTERS, IMAGE_TAG_SEPARATOR);
    }
}
