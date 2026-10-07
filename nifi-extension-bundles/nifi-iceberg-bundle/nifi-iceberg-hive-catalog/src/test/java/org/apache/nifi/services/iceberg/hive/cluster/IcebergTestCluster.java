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
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Kerberized cluster in containers sharing the network namespace of the Key Distribution Center container: Ranger Admin,
 * HDFS and Ozone with Ranger plugins and Hive Metastore. Independent containers start in parallel batches.
 */
public final class IcebergTestCluster implements AutoCloseable {
    public static final String NIFI_USER = "nifi";

    public static final String HDFS_WAREHOUSE_PATH = "/warehouse";

    public static final String HDFS_WAREHOUSE = MetastoreComponent.WAREHOUSE;

    public static final String OZONE_VOLUME = "iceberg";

    public static final String OZONE_BUCKET = "warehouse";

    public static final String OZONE_WAREHOUSE = OzoneComponent.location(OZONE_VOLUME, OZONE_BUCKET);

    /**
     * Ranger plugin in the Ozone Manager, enabled with -Dranger.ozone.plugin=true
     */
    public static final boolean OZONE_RANGER_ENABLED = Boolean.getBoolean("ranger.ozone.plugin");

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTestCluster.class);

    private static final String DIRECTORY_PROPERTY = "iceberg.cluster.directory";

    private static final String DIRECTORY_DEFAULT = "target";

    private static final String DIRECTORY_PREFIX = "iceberg-cluster-";

    private static final String KRB5_CONF_PROPERTY = "java.security.krb5.conf";

    private static final String PORT_BINDING_FORMAT = "%d:%d";

    private static final String CLIENT_SITE = "client-site.xml";

    private static final String WAREHOUSE_PROPERTY = "hive.metastore.warehouse.dir";

    private static final String HIVE_USER = "hive";

    private static final String WAREHOUSE_PERMISSION = "777";

    private static final List<String> SERVICE_USERS = List.of("hadoop", HIVE_USER);

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

    private static final String[] HDFS_ALL_ACCESS = {READ, WRITE, EXECUTE};

    private static final String[] HDFS_NIFI_READ_ACCESS = {READ, EXECUTE};

    private static final String[] OZONE_ALL_ACCESS = {ALL, READ, WRITE, CREATE, LIST, DELETE, READ_ACL, WRITE_ACL};

    private static final String[] OZONE_NIFI_READ_ACCESS = {READ, LIST, READ_ACL};

    private static final String[] OZONE_NIFI_WRITE_ACCESS = {READ, WRITE, CREATE, LIST, DELETE, READ_ACL};

    private static final String OZONE_VOLUME_KEYS = "%s/*/*".formatted(OZONE_VOLUME);

    private static final String OZONE_BUCKET_KEYS = "%s/%s/*".formatted(OZONE_VOLUME, OZONE_BUCKET);

    private final ClusterContext context;

    private final List<Startable> components = new ArrayList<>();

    private final Map<Storage, Long> writePolicies = new EnumMap<>(Storage.class);

    private final String previousKrb5Conf;

    private KdcComponent kdc;

    private RangerComponent ranger;

    private HdfsComponent hdfs;

    private OzoneComponent ozone;

    private MetastoreComponent metastore;

    private Path clientSite;

    /**
     * Storage systems with Ranger write policies for the nifi user
     */
    public enum Storage {
        HDFS,
        OZONE
    }

    /**
     * Cluster with keytabs and configuration files in a new directory under target or -Diceberg.cluster.directory
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

    public HdfsComponent hdfs() {
        return hdfs;
    }

    public OzoneComponent ozone() {
        return ozone;
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
     * Delete the Ranger policy allowing the nifi user to write and wait until the plugin activated the change
     *
     * @param storage Storage system
     * @throws Exception Thrown on Ranger failures or timeout
     */
    public void revokeNifiWrite(final Storage storage) throws Exception {
        final Long policyId = writePolicies.remove(storage);
        if (policyId != null) {
            ranger.delete(policyId);
            ranger.awaitPolicyRefresh(getServiceName(storage));
        }
    }

    /**
     * Create the Ranger policy allowing the nifi user to write when missing and wait until the plugin activated it
     *
     * @param storage Storage system
     * @throws Exception Thrown on Ranger failures or timeout
     */
    public void grantNifiWrite(final Storage storage) throws Exception {
        if (!writePolicies.containsKey(storage)) {
            final long policyId = switch (storage) {
                case HDFS -> ranger.allow(ranger.hdfsServiceName(), HDFS_WAREHOUSE_PATH, NIFI_USER, HDFS_ALL_ACCESS);
                case OZONE -> ranger.allow(ranger.ozoneServiceName(), OZONE_BUCKET_KEYS, NIFI_USER, OZONE_NIFI_WRITE_ACCESS);
            };
            writePolicies.put(storage, policyId);
        }
        ranger.awaitPolicyRefresh(getServiceName(storage));
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
        ozone = new OzoneComponent(context, OZONE_RANGER_ENABLED ? ranger : null);
        components.add(ranger);
        await(CompletableFuture.allOf(
                Startables.deepStart(ranger),
                CompletableFuture.runAsync(() -> hdfs.containers().getFirst().getDockerImageName()),
                CompletableFuture.runAsync(ozone::buildImage)
        ));
        ranger.createServices();
        createBasePolicies();
        LOGGER.info("Ranger Admin started with HDFS service [{}] and Ozone plugin enabled [{}]", ranger.hdfsServiceName(), OZONE_RANGER_ENABLED);
    }

    private void createBasePolicies() throws Exception {
        for (final String user : SERVICE_USERS) {
            ranger.allow(ranger.hdfsServiceName(), HDFS_ROOT, user, HDFS_ALL_ACCESS);
        }
        ranger.allow(ranger.hdfsServiceName(), HDFS_ROOT, NIFI_USER, HDFS_NIFI_READ_ACCESS);
        writePolicies.put(Storage.HDFS, ranger.allow(ranger.hdfsServiceName(), HDFS_WAREHOUSE_PATH, NIFI_USER, HDFS_ALL_ACCESS));
        if (OZONE_RANGER_ENABLED) {
            for (final String user : SERVICE_USERS) {
                ranger.allow(ranger.ozoneServiceName(), OZONE_VOLUME_KEYS, user, OZONE_ALL_ACCESS);
            }
            ranger.allow(ranger.ozoneServiceName(), OZONE_VOLUME_KEYS, NIFI_USER, OZONE_NIFI_READ_ACCESS);
            writePolicies.put(Storage.OZONE, ranger.allow(ranger.ozoneServiceName(), OZONE_BUCKET_KEYS, NIFI_USER, OZONE_NIFI_WRITE_ACCESS));
        }
    }

    private void startStorage() throws Exception {
        components.add(hdfs);
        components.add(ozone);
        await(Startables.deepStart(hdfs, ozone));
        ranger.awaitPolicyRefresh(ranger.hdfsServiceName());
        if (OZONE_RANGER_ENABLED) {
            ranger.awaitPolicyRefresh(ranger.ozoneServiceName());
        }
        LOGGER.info("HDFS and Ozone started");
    }

    private void bootstrapStorage() throws Exception {
        hdfs.createDirectory(HDFS_WAREHOUSE_PATH, HIVE_USER, WAREHOUSE_PERMISSION);
        ozone.createBucket(OZONE_VOLUME, OZONE_BUCKET, NIFI_USER);
        LOGGER.info("Storage bootstrapped [{}] [{}]", HDFS_WAREHOUSE, OZONE_WAREHOUSE);
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
