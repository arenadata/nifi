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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared settings of the cluster: Kerberos realm and principals, ports published by the Key Distribution Center
 * container and the host directory with keytabs and generated configuration files.
 */
public final class ClusterContext {
    public static final String HOST = "localhost";

    public static final String REALM = "NIFI.COM";

    public static final int KDC_PORT = 10088;

    public static final int NAME_NODE_PORT = 8020;

    public static final int DATA_NODE_PORT = 9866;

    public static final int METASTORE_PORT = 9083;

    public static final int OZONE_MANAGER_PORT = 9862;

    public static final int OZONE_RATIS_PORT = 9858;

    public static final int OZONE_GRPC_PORT = 9859;

    public static final int OZONE_SCM_CLIENT_PORT = 9860;

    public static final int RANGER_PORT = 6080;

    public static final int TRINO_PORT = 8080;

    public static final List<Integer> HOST_PORTS = List.of(KDC_PORT, NAME_NODE_PORT, DATA_NODE_PORT, METASTORE_PORT, OZONE_MANAGER_PORT,
            OZONE_RATIS_PORT, OZONE_GRPC_PORT, OZONE_SCM_CLIENT_PORT, RANGER_PORT);

    public static final String KEYTAB_DIRECTORY = "/etc/security/keytabs";

    public static final String KRB5_CONF_PATH = "/etc/krb5.conf";

    public static final String NAME_NODE_KEYTAB = "nn.keytab";

    public static final String DATA_NODE_KEYTAB = "dn.keytab";

    public static final String SCM_KEYTAB = "scm.keytab";

    public static final String OZONE_MANAGER_KEYTAB = "om.keytab";

    public static final String OZONE_DATA_NODE_KEYTAB = "ozone-dn.keytab";

    public static final String HIVE_KEYTAB = "hive.keytab";

    public static final String TRINO_KEYTAB = "trino.keytab";

    public static final String SPARK_KEYTAB = "spark.keytab";

    public static final String NIFI_KEYTAB = "nifi.keytab";

    public static final String ADMIN_KEYTAB = "admin.keytab";

    public static final String NAME_NODE_PRINCIPAL = servicePrincipal("nn");

    public static final String DATA_NODE_PRINCIPAL = servicePrincipal("dn");

    public static final String SCM_PRINCIPAL = servicePrincipal("scm");

    public static final String OZONE_MANAGER_PRINCIPAL = servicePrincipal("om");

    public static final String OZONE_DATA_NODE_PRINCIPAL = servicePrincipal("ozone-dn");

    public static final String HTTP_PRINCIPAL = servicePrincipal("HTTP");

    public static final String HIVE_PRINCIPAL = servicePrincipal("hive");

    public static final String TRINO_PRINCIPAL = servicePrincipal("trino");

    public static final String SPARK_PRINCIPAL = servicePrincipal("spark");

    public static final String NIFI_PRINCIPAL = userPrincipal("nifi");

    public static final String ADMIN_PRINCIPAL = userPrincipal("admin");

    public static final Map<String, List<String>> KEYTABS = keytabs();

    public static final String AUTH_TO_LOCAL = String.join("\n",
            "RULE:[2:$1@$0](nn@NIFI.COM)s/.*/hadoop/",
            "RULE:[2:$1@$0](dn@NIFI.COM)s/.*/hadoop/",
            "RULE:[2:$1@$0](scm@NIFI.COM)s/.*/hadoop/",
            "RULE:[2:$1@$0](om@NIFI.COM)s/.*/hadoop/",
            "RULE:[2:$1@$0](ozone-dn@NIFI.COM)s/.*/hadoop/",
            "RULE:[1:$1@$0](admin@NIFI.COM)s/.*/hadoop/",
            "DEFAULT"
    );

    public static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(10);

    public static final Duration POLL_INTERVAL = Duration.ofSeconds(2);

    private static final String KRB5_CONF_FORMAT = """
            [libdefaults]
                default_realm = %1$s
                dns_canonicalize_hostname = false
                rdns = false
                udp_preference_limit = 1
                ticket_lifetime = 24h
                renew_lifetime = 7d
                forwardable = true
            [realms]
                %1$s = {
                    kdc = %2$s:%3$d
                    admin_server = %2$s
                }
            [domain_realm]
                %2$s = %1$s
            """;

    private final Path directory;

    private volatile String networkOwnerId;

    public ClusterContext(final Path directory) {
        this.directory = directory;
        createDirectory(keytabDirectory());
        createDirectory(configurationDirectory());
    }

    /**
     * Host directory with keytabs created by the Key Distribution Center, mounted into containers at KEYTAB_DIRECTORY
     *
     * @return Keytab directory
     */
    public Path keytabDirectory() {
        return directory.resolve("keytabs");
    }

    /**
     * Host directory for generated configuration files
     *
     * @return Configuration directory
     */
    public Path configurationDirectory() {
        return directory.resolve("conf");
    }

    public Path keytab(final String fileName) {
        return keytabDirectory().resolve(fileName);
    }

    public String containerKeytab(final String fileName) {
        return "%s/%s".formatted(KEYTAB_DIRECTORY, fileName);
    }

    public String krb5Conf() {
        return KRB5_CONF_FORMAT.formatted(REALM, HOST, KDC_PORT);
    }

    public Path hostKrb5Conf() {
        return ClusterFiles.write(configurationDirectory().resolve("krb5.conf"), krb5Conf());
    }

    public void setNetworkOwnerId(final String containerId) {
        this.networkOwnerId = containerId;
    }

    /**
     * Docker network mode joining the network namespace of the Key Distribution Center container
     *
     * @return Network mode such as container:id
     */
    public String networkMode() {
        if (networkOwnerId == null) {
            throw new IllegalStateException("Key Distribution Center container not started");
        }
        return "container:%s".formatted(networkOwnerId);
    }

    /**
     * Core site properties shared by all Hadoop servers and clients in the cluster
     *
     * @param extra Additional properties overriding the shared properties
     * @return Core site properties
     */
    public Map<String, String> coreSite(final Map<String, String> extra) {
        final Map<String, String> properties = new LinkedHashMap<>();
        properties.put("fs.defaultFS", "hdfs://%s:%d".formatted(HOST, NAME_NODE_PORT));
        properties.put("hadoop.security.authentication", "kerberos");
        properties.put("hadoop.security.authorization", "true");
        properties.put("hadoop.security.auth_to_local", AUTH_TO_LOCAL);
        properties.put("hadoop.rpc.protection", "authentication");
        properties.put("hadoop.proxyuser.hive.hosts", "*");
        properties.put("hadoop.proxyuser.hive.groups", "*");
        properties.put("fs.ofs.impl", "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem");
        properties.put("ozone.om.address", "%s:%d".formatted(HOST, OZONE_MANAGER_PORT));
        properties.putAll(extra);
        return properties;
    }

    public static String servicePrincipal(final String service) {
        return "%s/%s@%s".formatted(service, HOST, REALM);
    }

    public static String userPrincipal(final String user) {
        return "%s@%s".formatted(user, REALM);
    }

    private static Map<String, List<String>> keytabs() {
        final Map<String, List<String>> keytabs = new LinkedHashMap<>();
        keytabs.put(NAME_NODE_KEYTAB, List.of(NAME_NODE_PRINCIPAL, HTTP_PRINCIPAL));
        keytabs.put(DATA_NODE_KEYTAB, List.of(DATA_NODE_PRINCIPAL, HTTP_PRINCIPAL));
        keytabs.put(SCM_KEYTAB, List.of(SCM_PRINCIPAL, HTTP_PRINCIPAL));
        keytabs.put(OZONE_MANAGER_KEYTAB, List.of(OZONE_MANAGER_PRINCIPAL, HTTP_PRINCIPAL));
        keytabs.put(OZONE_DATA_NODE_KEYTAB, List.of(OZONE_DATA_NODE_PRINCIPAL, HTTP_PRINCIPAL));
        keytabs.put(HIVE_KEYTAB, List.of(HIVE_PRINCIPAL));
        keytabs.put(TRINO_KEYTAB, List.of(TRINO_PRINCIPAL));
        keytabs.put(SPARK_KEYTAB, List.of(SPARK_PRINCIPAL));
        keytabs.put(NIFI_KEYTAB, List.of(NIFI_PRINCIPAL));
        keytabs.put(ADMIN_KEYTAB, List.of(ADMIN_PRINCIPAL));
        return Map.copyOf(keytabs);
    }

    private static void createDirectory(final Path path) {
        try {
            Files.createDirectories(path);
            if (path.getFileSystem().supportedFileAttributeViews().contains("posix")) {
                Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwxrwxrwx"));
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
