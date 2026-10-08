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

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Kerberized cluster in containers sharing the network namespace of the Key Distribution Center container: Ranger Admin,
 * HDFS and Ozone with Ranger plugins, Hive Metastore, Trino and Spark. Independent containers start in parallel batches.
 */
public final class IcebergTestCluster implements AutoCloseable {
    public static final String NIFI_USER = "nifi";

    public static final String HDFS_WAREHOUSE_PATH = "/warehouse";

    public static final String HDFS_WAREHOUSE = MetastoreComponent.WAREHOUSE;

    public static final String OZONE_VOLUME = "iceberg";

    public static final String OZONE_BUCKET = "warehouse";

    public static final String OZONE_WAREHOUSE = OzoneComponent.location(OZONE_VOLUME, OZONE_BUCKET);

    public static final String TRINO_USER = "trino";

    public static final String SPARK_USER = "spark";

    public static final List<String> DATA_USERS = List.of(NIFI_USER, TRINO_USER, SPARK_USER);

    private static final String HDFS_SECURED_PATH = "/secured";

    private static final String HDFS_SECURED = "hdfs://%s:%d%s".formatted(ClusterContext.HOST, ClusterContext.NAME_NODE_PORT, HDFS_SECURED_PATH);

    private static final String OZONE_SECURED_BUCKET = "secured";

    private static final String OZONE_SECURED = OzoneComponent.location(OZONE_VOLUME, OZONE_SECURED_BUCKET);

    private static final String OZONE_DENIED_BUCKET = "denied";

    private static final String BUCKET_RESOURCE_FORMAT = "%s/%s";

    public static final String OZONE_SECURED_BUCKET_RESOURCE = BUCKET_RESOURCE_FORMAT.formatted(OZONE_VOLUME, OZONE_SECURED_BUCKET);

    /**
     * HDFS root location where only the hadoop and hive users may write
     */
    private static final String HDFS_DENIED_LOCATION = "hdfs://%s:%d".formatted(ClusterContext.HOST, ClusterContext.NAME_NODE_PORT);

    /**
     * Location in a missing Ozone bucket that ofs creates on first use, which requires Ranger CREATE on the bucket
     */
    private static final String OZONE_DENIED_LOCATION = OzoneComponent.location(OZONE_VOLUME, OZONE_DENIED_BUCKET);

    private static final String OZONE_DENIED_BUCKET_RESOURCE = BUCKET_RESOURCE_FORMAT.formatted(OZONE_VOLUME, OZONE_DENIED_BUCKET);

    private static final String HDFS_DENIED_RESOURCE_FORMAT = "/%s.db";

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTestCluster.class);

    private static final String DIRECTORY_PROPERTY = "iceberg.cluster.directory";

    private static final String DIRECTORY_DEFAULT = System.getProperty("java.io.tmpdir");

    private static final String DIRECTORY_PREFIX = "iceberg-cluster-";

    private static final String KRB5_CONF_PROPERTY = "java.security.krb5.conf";

    private static final String PORT_BINDING_FORMAT = "%d:%d";

    private static final String CLIENT_SITE = "client-site.xml";

    private static final String WAREHOUSE_PROPERTY = "hive.metastore.warehouse.dir";

    private static final String HIVE_USER = "hive";

    private static final String WAREHOUSE_PERMISSION = "777";

    private static final String HADOOP_USER = "hadoop";

    private static final String ADMIN_USER = "admin";

    private static final List<String> HDFS_SERVICE_USERS = List.of(HADOOP_USER, HIVE_USER);

    private static final List<String> OZONE_SERVICE_USERS = List.of(HADOOP_USER, ADMIN_USER, HIVE_USER);

    private static final List<String> ENGINE_USERS = List.of(TRINO_USER, SPARK_USER);

    private static final String HDFS_ROOT = "/";

    private static final String READ = "read";

    private static final String WRITE = "write";

    private static final String EXECUTE = "execute";

    private static final String CREATE = "create";

    private static final String LIST = "list";

    private static final String DELETE = "delete";

    private static final String READ_ACL = "read_acl";

    private static final String WRITE_ACL = "write_acl";

    private static final String ALL = "all";

    private static final String[] HDFS_WRITE_ACCESS = {READ, WRITE, EXECUTE};

    private static final String[] HDFS_READ_ACCESS = {READ, EXECUTE};

    private static final String[] HDFS_DENY_WRITE_ACCESS = {WRITE};

    private static final String[] HDFS_DENY_READ_ACCESS = {READ};

    private static final String[] OZONE_ALL_ACCESS = {ALL, READ, WRITE, CREATE, LIST, DELETE, READ_ACL, WRITE_ACL};

    private static final String[] OZONE_NIFI_READ_ACCESS = {READ, LIST, READ_ACL};

    private static final String[] OZONE_READ_ACCESS = {READ, LIST};

    private static final String[] OZONE_WRITE_ACCESS = {READ, WRITE, CREATE, LIST, DELETE, READ_ACL};

    private static final String[] OZONE_DENY_WRITE_ACCESS = {WRITE, CREATE, DELETE};

    private static final String[] OZONE_DENY_READ_ACCESS = {READ};

    private static final String OZONE_VOLUME_KEYS = "%s/*/*".formatted(OZONE_VOLUME);

    private static final String OZONE_WAREHOUSE_KEYS = "%s/%s/*".formatted(OZONE_VOLUME, OZONE_BUCKET);

    private static final String OZONE_SECURED_KEYS = "%s/%s/*".formatted(OZONE_VOLUME, OZONE_SECURED_BUCKET);

    private static final String HDFS_RESOURCE_FORMAT = "%s/%s";

    private static final String OZONE_KEYS_FORMAT = "%s/%s/%s*";

    private static final String OZONE_SECURED_KEY_FORMAT = "%s/%s/%s";

    private static final String HDFS_DATA_RESOURCE_FORMAT = "%s/%s/data";

    private static final String OZONE_DATA_RESOURCE_FORMAT = "%s/%s/%s/data/*";

    private final ClusterContext context;

    private final List<Startable> components = new ArrayList<>();

    private final String previousKrb5Conf;

    private KdcComponent kdc;

    private RangerComponent ranger;

    private HdfsComponent hdfs;

    private OzoneComponent ozone;

    private MetastoreComponent metastore;

    private TrinoEngine trino;

    private SparkEngine spark;

    private Path clientSite;

    /**
     * Storage systems authorized by Ranger plugins
     */
    public enum Storage {
        HDFS,
        OZONE
    }

    /**
     * Ranger access levels mapped to the access types of each storage: READ allows reading and listing, WRITE allows
     * reading and modifying. Denying WRITE denies modifications only.
     */
    public enum Access {
        READ,
        WRITE
    }

    /**
     * Cluster with keytabs and configuration files in a new directory under the system temporary directory or -Diceberg.cluster.directory
     */
    public IcebergTestCluster() {
        final Path parent = Path.of(System.getProperty(DIRECTORY_PROPERTY, DIRECTORY_DEFAULT)).toAbsolutePath();
        try {
            Files.createDirectories(parent);
            this.context = new ClusterContext(Files.createTempDirectory(parent, DIRECTORY_PREFIX));
        } catch (final IOException e) {
            throw new UncheckedIOException("Cluster directory not created in [%s]".formatted(parent), e);
        }
        this.previousKrb5Conf = System.getProperty(KRB5_CONF_PROPERTY);
    }

    /**
     * Start all containers in dependency order and bootstrap storage, Ranger policies and client configuration
     *
     * @throws Exception Thrown when a component failed to start or bootstrap
     */
    public void start() throws Exception {
        LOGGER.info("Starting Iceberg test cluster in [{}]", context.configurationDirectory().getParent());
        System.setProperty(KRB5_CONF_PROPERTY, context.hostKrb5Conf().toString());

        startKeyDistributionCenter();
        startRanger();
        startStorage();
        bootstrapStorage();
        writeClientSite();
        startMetastore();
        startEngines();
        LOGGER.info("Iceberg test cluster started");
    }

    /**
     * Stop all started components in reverse start order and restore the krb5.conf system property
     */
    @Override
    public void close() {
        for (int index = components.size() - 1; index >= 0; index--) {
            final Startable component = components.get(index);
            try {
                component.stop();
            } catch (final RuntimeException e) {
                LOGGER.warn("Stopping [{}] failed", component.getClass().getSimpleName(), e);
            }
        }
        components.clear();

        UserGroupInformation.reset();
        if (previousKrb5Conf == null) {
            System.clearProperty(KRB5_CONF_PROPERTY);
        } else {
            System.setProperty(KRB5_CONF_PROPERTY, previousKrb5Conf);
        }
    }

    public TrinoEngine trino() {
        return trino;
    }

    public SparkEngine spark() {
        return spark;
    }

    /**
     * Host path of the client configuration with core site, HDFS, Ozone and Hive Metastore client properties
     *
     * @return Client configuration path
     */
    public Path clientSite() {
        return clientSite;
    }

    /**
     * Host path of a keytab created by the Key Distribution Center
     *
     * @param fileName Keytab file name such as nifi.keytab
     * @return Keytab path
     */
    public Path keytab(final String fileName) {
        return context.keytab(fileName);
    }

    /**
     * Remove the Ranger policy item allowing the nifi user to write in the warehouse without waiting for the plugin.
     * Trino and Spark keep their policy items for the warehouse.
     *
     * @param storage Storage system
     * @throws Exception Thrown on Ranger failures
     */
    public void revokeNifiWrite(final Storage storage) throws Exception {
        revoke(storage, getWarehouseResource(storage), NIFI_USER);
    }

    /**
     * Add the Ranger policy item allowing the nifi user to write in the warehouse without waiting for the plugin
     *
     * @param storage Storage system
     * @throws Exception Thrown on Ranger failures
     */
    public void grantNifiWrite(final Storage storage) throws Exception {
        allow(storage, getWarehouseResource(storage), NIFI_USER, Access.WRITE);
    }

    /**
     * Allow access for a user on a Ranger resource of the storage service without waiting for the plugin
     *
     * @param storage Storage system
     * @param resource HDFS path or Ozone volume/bucket/key pattern, such as returned from securedResource
     * @param user Short user name
     * @param access Access level
     * @return Policy identifier, shared by all users of the same resource
     * @throws Exception Thrown on Ranger failures
     */
    public long allow(final Storage storage, final String resource, final String user, final Access access) throws Exception {
        return ranger.allow(getServiceName(storage), resource, user, getAllowAccessTypes(storage, access));
    }

    /**
     * Allow access for a user on a path under the secured location without files and directories below it: a
     * non-recursive HDFS path or an exact Ozone key without wildcard. Does not wait for the plugin.
     *
     * @param storage Storage system
     * @param path Relative path such as namespace.db/table
     * @param user Short user name
     * @param access Access level
     * @return Policy identifier, shared by all users of the same resource
     * @throws Exception Thrown on Ranger failures
     */
    public long allowWithoutDescendants(final Storage storage, final String path, final String user, final Access access) throws Exception {
        final String[] accessTypes = getAllowAccessTypes(storage, access);
        return switch (storage) {
            case HDFS -> ranger.allow(getServiceName(storage), HDFS_RESOURCE_FORMAT.formatted(HDFS_SECURED_PATH, path), false, user, accessTypes);
            case OZONE -> ranger.allow(getServiceName(storage), OZONE_SECURED_KEY_FORMAT.formatted(OZONE_VOLUME, OZONE_SECURED_BUCKET, path), user, accessTypes);
        };
    }

    /**
     * Deny access for a user on a Ranger resource of the storage service without waiting for the plugin
     *
     * @param storage Storage system
     * @param resource HDFS path or Ozone volume/bucket/key pattern, such as returned from securedResource
     * @param user Short user name
     * @param access Access level to deny
     * @return Policy identifier, shared by all users of the same resource
     * @throws Exception Thrown on Ranger failures
     */
    public long deny(final Storage storage, final String resource, final String user, final Access access) throws Exception {
        return ranger.deny(getServiceName(storage), resource, user, getDenyAccessTypes(storage, access));
    }

    /**
     * Remove the allow and deny policy items of a user on a Ranger resource without waiting for the plugin
     *
     * @param storage Storage system
     * @param resource HDFS path or Ozone volume/bucket/key pattern
     * @param user Short user name
     * @throws Exception Thrown on Ranger failures
     */
    public void revoke(final Storage storage, final String resource, final String user) throws Exception {
        ranger.revoke(getServiceName(storage), resource, user);
    }

    /**
     * Enable or disable a Ranger policy without waiting for the plugin
     *
     * @param policyId Policy identifier
     * @param enabled Enabled status
     * @throws Exception Thrown on Ranger failures
     */
    public void setPolicyEnabled(final long policyId, final boolean enabled) throws Exception {
        ranger.setEnabled(policyId, enabled);
    }

    /**
     * Delete a Ranger policy without waiting for the plugin, ignoring policies that do not exist
     *
     * @param policyId Policy identifier
     * @throws Exception Thrown on Ranger failures
     */
    public void deletePolicy(final long policyId) throws Exception {
        ranger.delete(policyId);
    }

    /**
     * Wait until the Ranger plugin of the storage activated the latest policy version
     *
     * @param storage Storage system
     * @throws Exception Thrown on Ranger failures or timeout
     */
    public void awaitPolicyRefresh(final Storage storage) throws Exception {
        ranger.awaitPolicyRefresh(getServiceName(storage));
    }

    /**
     * Ranger audit events of the storage plugin as JSON lines in write order
     *
     * @param storage Storage system
     * @return Audit lines
     * @throws Exception Thrown when reading the audit files failed
     */
    public List<String> auditLines(final Storage storage) throws Exception {
        return switch (storage) {
            case HDFS -> hdfs.auditLines();
            case OZONE -> ozone.auditLines();
        };
    }

    /**
     * Location without Ranger path or key policies for Trino and Spark, where the nifi user may write. On Ozone, Trino
     * and Spark may only read and list the volume and the bucket.
     *
     * @param storage Storage system
     * @return Location such as hdfs://localhost:8020/secured
     */
    public static String securedLocation(final Storage storage) {
        return switch (storage) {
            case HDFS -> HDFS_SECURED;
            case OZONE -> OZONE_SECURED;
        };
    }

    /**
     * Ranger resource for a path under the secured location and everything below it
     *
     * @param storage Storage system
     * @param path Relative path such as namespace.db/table
     * @return Recursive HDFS path or Ozone volume/bucket/key pattern
     */
    public static String securedResource(final Storage storage, final String path) {
        return getResource(storage, HDFS_SECURED_PATH, OZONE_SECURED_BUCKET, path);
    }

    /**
     * Ranger resource for the data directory of a table under the secured location and everything below it
     *
     * @param storage Storage system
     * @param tablePath Relative table path such as namespace.db/table
     * @return Recursive HDFS path or Ozone volume/bucket/key pattern
     */
    public static String securedDataResource(final Storage storage, final String tablePath) {
        return switch (storage) {
            case HDFS -> HDFS_DATA_RESOURCE_FORMAT.formatted(HDFS_SECURED_PATH, tablePath);
            case OZONE -> OZONE_DATA_RESOURCE_FORMAT.formatted(OZONE_VOLUME, OZONE_SECURED_BUCKET, tablePath);
        };
    }

    /**
     * Ranger resource for a path under the warehouse location and everything below it
     *
     * @param storage Storage system
     * @param path Relative path such as namespace.db/table
     * @return Recursive HDFS path or Ozone volume/bucket/key pattern
     */
    public static String warehouseResource(final Storage storage, final String path) {
        return getResource(storage, HDFS_WAREHOUSE_PATH, OZONE_BUCKET, path);
    }

    /**
     * Location without Ranger policies allowing the nifi user to create directories
     *
     * @param storage Storage system
     * @return HDFS root or a location in a missing Ozone bucket
     */
    public static String deniedLocation(final Storage storage) {
        return switch (storage) {
            case HDFS -> HDFS_DENIED_LOCATION;
            case OZONE -> OZONE_DENIED_LOCATION;
        };
    }

    /**
     * Ranger audit resource of a namespace denied in the location returned from deniedLocation
     *
     * @param storage Storage system
     * @param namespaceName Namespace name
     * @return HDFS namespace directory or the missing Ozone bucket
     */
    public static String deniedResource(final Storage storage, final String namespaceName) {
        return switch (storage) {
            case HDFS -> HDFS_DENIED_RESOURCE_FORMAT.formatted(namespaceName);
            case OZONE -> OZONE_DENIED_BUCKET_RESOURCE;
        };
    }

    private void startKeyDistributionCenter() {
        kdc = new KdcComponent(context);
        kdc.container().setPortBindings(ClusterContext.HOST_PORTS.stream().map(port -> PORT_BINDING_FORMAT.formatted(port, port)).toList());
        components.add(kdc);
        kdc.start();
        ClusterContext.KEYTABS.forEach((fileName, principals) -> kdc.createKeytab(fileName, principals.toArray(String[]::new)));
        LOGGER.info("Key Distribution Center started with keytabs {}", ClusterContext.KEYTABS.keySet());
    }

    private void startRanger() throws Exception {
        ranger = new RangerComponent(context);
        hdfs = new HdfsComponent(context, ranger);
        ozone = new OzoneComponent(context, ranger);
        components.add(ranger);
        await(CompletableFuture.allOf(
                Startables.deepStart(ranger),
                CompletableFuture.runAsync(() -> hdfs.containers().getFirst().getDockerImageName()),
                CompletableFuture.runAsync(ozone::buildImage)
        ));
        ranger.createServices();
        createBasePolicies();
        LOGGER.info("Ranger Admin started with services [{}] [{}]", ranger.hdfsServiceName(), ranger.ozoneServiceName());
    }

    private void createBasePolicies() throws Exception {
        final String hdfsService = ranger.hdfsServiceName();
        for (final String user : HDFS_SERVICE_USERS) {
            ranger.allow(hdfsService, HDFS_ROOT, user, HDFS_WRITE_ACCESS);
        }
        ranger.allow(hdfsService, HDFS_ROOT, NIFI_USER, HDFS_READ_ACCESS);
        for (final String user : DATA_USERS) {
            ranger.allow(hdfsService, HDFS_WAREHOUSE_PATH, user, HDFS_WRITE_ACCESS);
        }
        ranger.allow(hdfsService, HDFS_SECURED_PATH, NIFI_USER, HDFS_WRITE_ACCESS);

        final String ozoneService = ranger.ozoneServiceName();
        for (final String user : OZONE_SERVICE_USERS) {
            ranger.allow(ozoneService, OZONE_VOLUME_KEYS, user, OZONE_ALL_ACCESS);
        }
        ranger.allow(ozoneService, OZONE_VOLUME_KEYS, NIFI_USER, OZONE_NIFI_READ_ACCESS);
        for (final String user : ENGINE_USERS) {
            ranger.allow(ozoneService, OZONE_VOLUME, user, OZONE_READ_ACCESS);
            ranger.allow(ozoneService, OZONE_SECURED_BUCKET_RESOURCE, user, OZONE_READ_ACCESS);
        }
        for (final String user : DATA_USERS) {
            ranger.allow(ozoneService, OZONE_WAREHOUSE_KEYS, user, OZONE_WRITE_ACCESS);
        }
        ranger.allow(ozoneService, OZONE_SECURED_KEYS, NIFI_USER, OZONE_WRITE_ACCESS);
    }

    private void startStorage() throws Exception {
        components.add(hdfs);
        components.add(ozone);
        await(Startables.deepStart(hdfs, ozone));
        ranger.awaitPolicyRefresh(ranger.hdfsServiceName());
        ranger.awaitPolicyRefresh(ranger.ozoneServiceName());
        LOGGER.info("HDFS and Ozone started with active Ranger policies");
    }

    private void bootstrapStorage() throws Exception {
        hdfs.createDirectory(HDFS_WAREHOUSE_PATH, HIVE_USER, WAREHOUSE_PERMISSION);
        hdfs.createDirectory(HDFS_SECURED_PATH, HIVE_USER, WAREHOUSE_PERMISSION);
        ozone.createBucket(OZONE_VOLUME, OZONE_BUCKET);
        ozone.createBucket(OZONE_VOLUME, OZONE_SECURED_BUCKET);
        LOGGER.info("Storage bootstrapped [{}] [{}] [{}] [{}]", HDFS_WAREHOUSE, HDFS_SECURED, OZONE_WAREHOUSE, OZONE_SECURED);
    }

    private void writeClientSite() {
        final Map<String, String> properties = new LinkedHashMap<>(HdfsComponent.clientProperties());
        properties.putAll(OzoneComponent.clientProperties());
        properties.putAll(MetastoreComponent.clientProperties());
        properties.put(WAREHOUSE_PROPERTY, HDFS_WAREHOUSE);
        clientSite = ClusterFiles.write(context.configurationDirectory().resolve(CLIENT_SITE), ClusterFiles.hadoopXml(context.coreSite(properties)));
    }

    private void startMetastore() {
        metastore = new MetastoreComponent(context);
        components.add(metastore);
        metastore.start();
        LOGGER.info("Hive Metastore started [{}]", MetastoreComponent.METASTORE_URI);
    }

    private void startEngines() {
        trino = new TrinoEngine(context, clientSite);
        spark = new SparkEngine(context, clientSite);
        components.add(trino);
        components.add(spark);
        await(Startables.deepStart(trino, spark));
        LOGGER.info("Trino and Spark started");
    }

    private static String[] getAllowAccessTypes(final Storage storage, final Access access) {
        return switch (storage) {
            case HDFS -> access == Access.READ ? HDFS_READ_ACCESS : HDFS_WRITE_ACCESS;
            case OZONE -> access == Access.READ ? OZONE_READ_ACCESS : OZONE_WRITE_ACCESS;
        };
    }

    private static String[] getDenyAccessTypes(final Storage storage, final Access access) {
        return switch (storage) {
            case HDFS -> access == Access.READ ? HDFS_DENY_READ_ACCESS : HDFS_DENY_WRITE_ACCESS;
            case OZONE -> access == Access.READ ? OZONE_DENY_READ_ACCESS : OZONE_DENY_WRITE_ACCESS;
        };
    }

    private static String getResource(final Storage storage, final String hdfsPath, final String bucket, final String path) {
        return switch (storage) {
            case HDFS -> HDFS_RESOURCE_FORMAT.formatted(hdfsPath, path);
            case OZONE -> OZONE_KEYS_FORMAT.formatted(OZONE_VOLUME, bucket, path);
        };
    }

    private static String getWarehouseResource(final Storage storage) {
        return switch (storage) {
            case HDFS -> HDFS_WAREHOUSE_PATH;
            case OZONE -> OZONE_WAREHOUSE_KEYS;
        };
    }

    private String getServiceName(final Storage storage) {
        return switch (storage) {
            case HDFS -> ranger.hdfsServiceName();
            case OZONE -> ranger.ozoneServiceName();
        };
    }

    private static void await(final CompletableFuture<?> future) {
        try {
            future.join();
        } catch (final CompletionException e) {
            if (e.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw e;
        }
    }
}
