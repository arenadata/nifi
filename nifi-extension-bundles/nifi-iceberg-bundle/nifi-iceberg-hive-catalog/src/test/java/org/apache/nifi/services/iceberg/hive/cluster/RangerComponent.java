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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Apache Ranger Admin with its PostgreSQL database, managed through the public REST API as admin. Plugins download
 * policies with HTTP Basic authentication and write audit events as JSON lines under /tmp/ranger-audit/&lt;type&gt;.
 */
public final class RangerComponent implements Startable {
    static final String HDFS_TYPE = "hdfs";

    static final String OZONE_TYPE = "ozone";

    static final String HDFS_SERVICE = "dev_hdfs";

    static final String OZONE_SERVICE = "dev_ozone";

    static final String ADMIN_USER = "admin";

    static final String ADMIN_PASSWORD = "rangerR0cks!";

    static final String VERSION = "2.9.0";

    static final String AUDIT_DIRECTORY_FORMAT = "/tmp/ranger-audit/%s";

    static final String POLICY_CACHE_DIRECTORY_FORMAT = "/tmp/ranger-cache/%s";

    static final String SECURITY_FILE_FORMAT = "ranger-%s-security.xml";

    static final String AUDIT_FILE_FORMAT = "ranger-%s-audit.xml";

    private static final Logger LOGGER = LoggerFactory.getLogger(RangerComponent.class);

    private static final String ADMIN_IMAGE_PROPERTY = "ranger.image";

    private static final String ADMIN_IMAGE_DEFAULT = "apache/ranger:" + VERSION;

    private static final String DATABASE_IMAGE_PROPERTY = "ranger.db.image";

    private static final String DATABASE_IMAGE_DEFAULT = "apache/ranger-db:" + VERSION;

    private static final String ADMIN_LOG_PREFIX = "ranger-admin";

    private static final String DATABASE_LOG_PREFIX = "ranger-db";

    private static final String POSTGRES_PASSWORD_VARIABLE = "POSTGRES_PASSWORD";

    private static final String DATABASE_USER_VARIABLE = "RANGER_DB_USER";

    private static final String DATABASE_PASSWORD_VARIABLE = "RANGER_DB_PASSWORD";

    private static final String DATABASE_USER = "rangeradmin";

    private static final String DATABASE_READY_PATTERN = "(?s).*database system is ready to accept connections.*";

    private static final int DATABASE_READY_TIMES = 2;

    private static final String SETUP_FAILED_MESSAGE = "Ranger Admin Setup Script didn't complete proper execution.";

    private static final String START_FAILED_MESSAGE = "Apache Ranger Admin Service failed to start!";

    private static final String ADMIN_STARTED_PATTERN = "(?s).*(Apache Ranger Admin Service with pid .* has started"
            + "|Apache Ranger Admin Service failed to start|Ranger Admin Setup Script didn't complete proper execution).*";

    private static final String INSTALL_PROPERTIES_RESOURCE = "ranger/ranger-admin-install.properties";

    private static final String INSTALL_PROPERTIES_PATH = "/home/ranger/scripts/ranger-admin-install.properties";

    private static final String DIAGNOSTICS_COMMAND = "tail -n 200 /opt/ranger/admin/logfile /var/log/ranger/*.log /var/log/ranger/catalina.out 2>&1 || true";

    private static final String SHELL = "sh";

    private static final String SHELL_COMMAND_OPTION = "-c";

    private static final String ARCHIVE_URL_FORMAT = "https://archive.apache.org/dist/ranger/%1$s/plugins/%2$s/ranger-%1$s-%2$s-plugin.tar.gz";

    private static final String BASE_URL = "http://%s:%d".formatted(ClusterContext.HOST, ClusterContext.RANGER_PORT);

    private static final String SERVICE_DEFINITION_PATH_FORMAT = "/service/public/v2/api/servicedef/name/%s";

    private static final String SERVICE_PATH = "/service/public/v2/api/service";

    private static final String SERVICE_BY_NAME_PATH_FORMAT = "/service/public/v2/api/service/name/%s";

    private static final String SERVICE_POLICIES_PATH_FORMAT = "/service/public/v2/api/service/%s/policy";

    private static final String POLICY_PATH = "/service/public/v2/api/policy";

    private static final String POLICY_BY_ID_PATH_FORMAT = POLICY_PATH + "/%d";

    private static final String CREATE_PRINCIPALS_QUERY = "?createPrincipalsIfAbsent=true";

    private static final String PLUGINS_INFO_PATH_FORMAT = "/service/plugins/plugins/info?serviceName=%s";

    private static final String GET = "GET";

    private static final String POST = "POST";

    private static final String PUT = "PUT";

    private static final String DELETE = "DELETE";

    private static final String AUTHORIZATION_HEADER = "Authorization";

    private static final String AUTHORIZATION = "Basic " + Base64.getEncoder().encodeToString("%s:%s".formatted(ADMIN_USER, ADMIN_PASSWORD).getBytes(StandardCharsets.UTF_8));

    private static final String ACCEPT_HEADER = "Accept";

    private static final String CONTENT_TYPE_HEADER = "Content-Type";

    private static final String JSON_CONTENT_TYPE = "application/json";

    private static final String REQUEST_FAILED_FORMAT = "Ranger Admin %s %s failed with HTTP status [%d]: %s";

    private static final int HTTP_OK = 200;

    private static final int HTTP_MULTIPLE_CHOICES = 300;

    private static final int HTTP_NOT_FOUND = 404;

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(60);

    private static final Duration POLICY_REFRESH_TIMEOUT = Duration.ofMinutes(2);

    private static final String POLL_INTERVAL = "5000";

    private static final String CONNECTION_TIMEOUT = "10000";

    private static final String READ_TIMEOUT = "30000";

    private static final String AUDIT_BATCH_INTERVAL = "1000";

    private static final String ENABLED = "true";

    private static final String DISABLED = "false";

    private static final String PLUGIN_PREFIX_FORMAT = "ranger.plugin.%s";

    private static final String REST_CLIENT = "org.apache.ranger.admin.client.RangerAdminRESTClient";

    private static final String SERVICE_USER = "hadoop";

    private static final String SIMPLE_AUTHENTICATION = "simple";

    private static final String USERNAME_CONFIG = "username";

    private static final String PASSWORD_CONFIG = "password";

    private static final String AUTHENTICATION_CONFIG = "hadoop.security.authentication";

    private static final String OWNER_USER = "{OWNER}";

    private static final String DEFAULT_POLICY_PREFIX = "all";

    private static final String POLICY_NAME_FORMAT = "cluster-%s-%s";

    private static final String PATH_SEPARATOR = "/";

    private static final String LEADING_SEPARATORS = "^/+";

    private static final int OZONE_RESOURCE_LEVELS = 3;

    private static final List<String> OZONE_RESOURCES = List.of("volume", "bucket", "key");

    private static final String PATH_RESOURCE = "path";

    private static final String NAME = "name";

    private static final String TYPE = "type";

    private static final String ID = "id";

    private static final String SERVICE = "service";

    private static final String RESOURCES = "resources";

    private static final String VALUES = "values";

    private static final String IS_RECURSIVE = "isRecursive";

    private static final String IS_EXCLUDES = "isExcludes";

    private static final String IS_ENABLED = "isEnabled";

    private static final String POLICY_ITEMS = "policyItems";

    private static final String POLICY_TYPE = "policyType";

    private static final String ZONE_NAME = "zoneName";

    private static final String USERS = "users";

    private static final String ACCESSES = "accesses";

    private static final String POLICY_VERSION = "policyVersion";

    private static final String PLUGIN_INFO_LIST = "pluginInfoList";

    private static final String SERVICE_NAME = "serviceName";

    private static final String HOST_NAME = "hostName";

    private static final String APP_TYPE = "appType";

    private static final String INFO = "info";

    private static final String POLICY_ACTIVE_VERSION = "policyActiveVersion";

    private static final long UNKNOWN_VERSION = -1;

    private final GenericContainer<?> database;

    private final GenericContainer<?> admin;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build();

    public RangerComponent(final ClusterContext context) {
        this.database = new GenericContainer<>(DockerImageName.parse(System.getProperty(DATABASE_IMAGE_PROPERTY, DATABASE_IMAGE_DEFAULT)))
                .withEnv(POSTGRES_PASSWORD_VARIABLE, ADMIN_PASSWORD)
                .withEnv(DATABASE_USER_VARIABLE, DATABASE_USER)
                .withEnv(DATABASE_PASSWORD_VARIABLE, ADMIN_PASSWORD)
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withNetworkMode(context.networkMode()))
                .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix(DATABASE_LOG_PREFIX))
                .waitingFor(Wait.forLogMessage(DATABASE_READY_PATTERN, DATABASE_READY_TIMES).withStartupTimeout(ClusterContext.STARTUP_TIMEOUT));

        final Map<String, String> installVariables = Map.of("host", ClusterContext.HOST, "port", Integer.toString(ClusterContext.RANGER_PORT));
        this.admin = new GenericContainer<>(DockerImageName.parse(System.getProperty(ADMIN_IMAGE_PROPERTY, ADMIN_IMAGE_DEFAULT)))
                .withEnv(POSTGRES_PASSWORD_VARIABLE, ADMIN_PASSWORD)
                .withEnv(DATABASE_USER_VARIABLE, DATABASE_USER)
                .withEnv(DATABASE_PASSWORD_VARIABLE, ADMIN_PASSWORD)
                .withCopyToContainer(Transferable.of(ClusterFiles.resource(INSTALL_PROPERTIES_RESOURCE, installVariables)), INSTALL_PROPERTIES_PATH)
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withNetworkMode(context.networkMode()))
                .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix(ADMIN_LOG_PREFIX))
                .waitingFor(Wait.forLogMessage(ADMIN_STARTED_PATTERN, 1).withStartupTimeout(ClusterContext.STARTUP_TIMEOUT));
    }

    /**
     * Start PostgreSQL, then Ranger Admin, and wait until the Ranger Admin REST API answers authenticated requests.
     * The first start runs the database schema setup and takes a few minutes.
     */
    @Override
    public void start() {
        database.start();
        admin.start();
        final String logs = admin.getLogs();
        if (logs.contains(SETUP_FAILED_MESSAGE) || logs.contains(START_FAILED_MESSAGE)) {
            logDiagnostics();
            throw new IllegalStateException("Ranger Admin setup or start failed, see container log with prefix [%s]".formatted(ADMIN_LOG_PREFIX));
        }
        try {
            ClusterFiles.waitUntil("Ranger Admin REST API", ClusterContext.STARTUP_TIMEOUT,
                    () -> request(GET, SERVICE_DEFINITION_PATH_FORMAT.formatted(HDFS_TYPE), null));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Waiting for Ranger Admin interrupted", e);
        } catch (final IllegalStateException e) {
            logDiagnostics();
            throw e;
        }
    }

    @Override
    public void stop() {
        admin.stop();
        database.stop();
    }

    public String hdfsServiceName() {
        return HDFS_SERVICE;
    }

    public String ozoneServiceName() {
        return OZONE_SERVICE;
    }

    /**
     * Create the HDFS and Ozone services when missing and remove the default Ozone policy items granting access to
     * resource owners, so that bucket access is controlled by explicit policies only
     *
     * @throws Exception Thrown on REST failures
     */
    public void createServices() throws Exception {
        final Map<String, String> hdfsConfigs = new LinkedHashMap<>();
        hdfsConfigs.put(USERNAME_CONFIG, SERVICE_USER);
        hdfsConfigs.put(PASSWORD_CONFIG, SERVICE_USER);
        hdfsConfigs.put("fs.default.name", "hdfs://%s:%d".formatted(ClusterContext.HOST, ClusterContext.NAME_NODE_PORT));
        hdfsConfigs.put("hadoop.security.authorization", ENABLED);
        hdfsConfigs.put(AUTHENTICATION_CONFIG, SIMPLE_AUTHENTICATION);
        ensureService(HDFS_SERVICE, HDFS_TYPE, hdfsConfigs);

        final Map<String, String> ozoneConfigs = new LinkedHashMap<>();
        ozoneConfigs.put(USERNAME_CONFIG, SERVICE_USER);
        ozoneConfigs.put(PASSWORD_CONFIG, SERVICE_USER);
        ozoneConfigs.put("ozone.om.http-address", "http://%s:9874".formatted(ClusterContext.HOST));
        ozoneConfigs.put(AUTHENTICATION_CONFIG, SIMPLE_AUTHENTICATION);
        ensureService(OZONE_SERVICE, OZONE_TYPE, ozoneConfigs);

        removeOwnerPolicyItems(OZONE_SERVICE);
    }

    /**
     * Allow access for a user on a resource, adding a policy item to an existing policy with the same resources
     *
     * @param service Service name such as dev_hdfs or dev_ozone
     * @param resourcePath HDFS path, recursive, or Ozone volume, volume/bucket or volume/bucket/key pattern
     * @param user Short user name such as nifi
     * @param accessTypes Access types such as read, write, execute for HDFS or read, write, create, list, delete, read_acl, all for Ozone
     * @return Policy identifier
     * @throws Exception Thrown on REST failures
     */
    public long allow(final String service, final String resourcePath, final String user, final String... accessTypes) throws Exception {
        if (accessTypes.length == 0) {
            throw new IllegalArgumentException("Access types required");
        }
        final String serviceType = request(GET, SERVICE_BY_NAME_PATH_FORMAT.formatted(encode(service)), null).path(TYPE).asText();
        final ObjectNode resources = createResources(serviceType, resourcePath);
        final ObjectNode policyItem = createPolicyItem(user, accessTypes);

        final Optional<JsonNode> existing = findPolicy(service, resources);
        final JsonNode policy;
        if (existing.isPresent()) {
            final ObjectNode updated = (ObjectNode) existing.get();
            final JsonNode items = updated.path(POLICY_ITEMS);
            final ArrayNode policyItems = items.isArray() ? (ArrayNode) items : updated.putArray(POLICY_ITEMS);
            policyItems.add(policyItem);
            policy = request(PUT, POLICY_BY_ID_PATH_FORMAT.formatted(updated.path(ID).asLong()) + CREATE_PRINCIPALS_QUERY, updated);
        } else {
            final ObjectNode created = objectMapper.createObjectNode();
            created.put(SERVICE, service);
            created.put(NAME, POLICY_NAME_FORMAT.formatted(user, UUID.randomUUID()));
            created.put("description", "Created by the Iceberg test cluster");
            created.put(IS_ENABLED, true);
            created.put("isAuditEnabled", true);
            created.set(RESOURCES, resources);
            created.putArray(POLICY_ITEMS).add(policyItem);
            policy = request(POST, POLICY_PATH + CREATE_PRINCIPALS_QUERY, created);
        }
        final long policyId = policy.path(ID).asLong(UNKNOWN_VERSION);
        if (policyId == UNKNOWN_VERSION) {
            throw new IllegalStateException("Policy identifier not found in Ranger response [%s]".formatted(policy));
        }
        LOGGER.info("Ranger policy [{}] allows [{}] for [{}] on [{}] in [{}]", policyId, String.join(",", accessTypes), user, resourcePath, service);
        return policyId;
    }

    /**
     * Delete a policy, ignoring policies that do not exist
     *
     * @param policyId Policy identifier
     * @throws Exception Thrown on REST failures
     */
    public void delete(final long policyId) throws Exception {
        final String path = POLICY_BY_ID_PATH_FORMAT.formatted(policyId);
        final HttpResponse<String> response = send(DELETE, path, null);
        if (!isSuccessful(response.statusCode()) && response.statusCode() != HTTP_NOT_FOUND) {
            throw new IOException(REQUEST_FAILED_FORMAT.formatted(DELETE, path, response.statusCode(), response.body()));
        }
        LOGGER.info("Ranger policy [{}] deleted", policyId);
    }

    /**
     * Wait until at least one plugin of the service is registered and every plugin of the service activated the
     * latest policy version
     *
     * @param serviceName Service name
     * @throws Exception Thrown on timeout or REST failures
     */
    public void awaitPolicyRefresh(final String serviceName) throws Exception {
        ClusterFiles.waitUntil("Ranger policy refresh for [%s]".formatted(serviceName), POLICY_REFRESH_TIMEOUT, () -> {
            if (getCurrentPlugins(serviceName) == 0) {
                throw new IllegalStateException("No Ranger plugins registered for [%s]".formatted(serviceName));
            }
        });
    }

    /**
     * Plugin configuration files for the configuration directory on the class path of the plugin host
     *
     * @param serviceType Service type such as hdfs or ozone
     * @param serviceName Service name such as dev_hdfs or dev_ozone
     * @return File name to file content
     */
    public Map<String, String> pluginFiles(final String serviceType, final String serviceName) {
        final String prefix = PLUGIN_PREFIX_FORMAT.formatted(serviceType);
        final Map<String, String> security = new LinkedHashMap<>();
        security.put(prefix + ".service.name", serviceName);
        security.put(prefix + ".policy.source.impl", REST_CLIENT);
        security.put(prefix + ".policy.rest.url", BASE_URL);
        security.put(prefix + ".policy.rest.client.username", ADMIN_USER);
        security.put(prefix + ".policy.rest.client.password", ADMIN_PASSWORD);
        security.put(prefix + ".policy.rest.client.connection.timeoutMs", CONNECTION_TIMEOUT);
        security.put(prefix + ".policy.rest.client.read.timeoutMs", READ_TIMEOUT);
        security.put(prefix + ".forceNonKerberos", ENABLED);
        security.put(prefix + ".policy.pollIntervalMs", POLL_INTERVAL);
        security.put(prefix + ".policy.cache.dir", POLICY_CACHE_DIRECTORY_FORMAT.formatted(serviceType));
        if (HDFS_TYPE.equals(serviceType)) {
            security.put("xasecure.add-hadoop-authorization", DISABLED);
            security.put("xasecure.auditlog.xasecureAcl.name", "ranger-acl");
            security.put("xasecure.auditlog.hadoopAcl.name", "hadoop-acl");
        }

        final Map<String, String> audit = new LinkedHashMap<>();
        audit.put("xasecure.audit.is.enabled", ENABLED);
        audit.put("xasecure.audit.destination.file", ENABLED);
        audit.put("xasecure.audit.destination.file.dir", AUDIT_DIRECTORY_FORMAT.formatted(serviceType));
        audit.put("xasecure.audit.destination.file.batch.batch.interval.ms", AUDIT_BATCH_INTERVAL);
        audit.put("xasecure.audit.destination.solr", DISABLED);
        audit.put("xasecure.audit.destination.hdfs", DISABLED);
        audit.put("xasecure.audit.destination.log4j", DISABLED);
        audit.put("xasecure.audit.solr.is.enabled", DISABLED);
        audit.put("xasecure.audit.hdfs.is.enabled", DISABLED);
        audit.put("xasecure.audit.log4j.is.enabled", DISABLED);
        audit.put("xasecure.audit.kafka.is.enabled", DISABLED);

        final Map<String, String> files = new LinkedHashMap<>();
        files.put(SECURITY_FILE_FORMAT.formatted(serviceType), ClusterFiles.hadoopXml(security));
        files.put(AUDIT_FILE_FORMAT.formatted(serviceType), ClusterFiles.hadoopXml(audit));
        return files;
    }

    /**
     * Download location of the Ranger plugin archive with jars and install scripts
     *
     * @param serviceType Service type such as hdfs or ozone
     * @return Archive URL
     */
    public static String pluginArchiveUrl(final String serviceType) {
        return ARCHIVE_URL_FORMAT.formatted(VERSION, serviceType);
    }

    private void ensureService(final String name, final String type, final Map<String, String> configs) throws IOException, InterruptedException {
        final String path = SERVICE_BY_NAME_PATH_FORMAT.formatted(encode(name));
        if (send(GET, path, null).statusCode() == HTTP_OK) {
            LOGGER.info("Ranger service [{}] found", name);
            return;
        }
        final ObjectNode service = objectMapper.createObjectNode();
        service.put(NAME, name);
        service.put("displayName", name);
        service.put(TYPE, type);
        service.put(IS_ENABLED, true);
        final ObjectNode serviceConfigs = service.putObject("configs");
        configs.forEach(serviceConfigs::put);
        final HttpResponse<String> response = send(POST, SERVICE_PATH, service);
        if (isSuccessful(response.statusCode())) {
            LOGGER.info("Ranger service [{}] created", name);
        } else if (send(GET, path, null).statusCode() == HTTP_OK) {
            LOGGER.info("Ranger service [{}] created concurrently", name);
        } else {
            throw new IOException("Ranger service [%s] creation failed with HTTP status [%d]: %s".formatted(name, response.statusCode(), response.body()));
        }
    }

    private void removeOwnerPolicyItems(final String service) throws IOException, InterruptedException {
        for (final JsonNode policy : request(GET, SERVICE_POLICIES_PATH_FORMAT.formatted(encode(service)), null)) {
            if (!policy.path(NAME).asText().startsWith(DEFAULT_POLICY_PREFIX)) {
                continue;
            }
            final ArrayNode items = objectMapper.createArrayNode();
            boolean changed = false;
            for (final JsonNode item : policy.path(POLICY_ITEMS)) {
                if (containsText(item.path(USERS), OWNER_USER)) {
                    changed = true;
                } else {
                    items.add(item);
                }
            }
            if (changed) {
                ((ObjectNode) policy).set(POLICY_ITEMS, items);
                request(PUT, POLICY_BY_ID_PATH_FORMAT.formatted(policy.path(ID).asLong()), policy);
                LOGGER.info("Ranger policy [{}] in [{}] no longer grants access to resource owners", policy.path(NAME).asText(), service);
            }
        }
    }

    private Optional<JsonNode> findPolicy(final String service, final ObjectNode resources) throws IOException, InterruptedException {
        final String signature = getResourceSignature(resources);
        for (final JsonNode policy : request(GET, SERVICE_POLICIES_PATH_FORMAT.formatted(encode(service)), null)) {
            final boolean accessPolicy = policy.path(POLICY_TYPE).asInt(0) == 0;
            final boolean noZone = policy.path(ZONE_NAME).asText().isEmpty();
            final boolean enabled = policy.path(IS_ENABLED).asBoolean(true);
            if (accessPolicy && noZone && enabled && signature.equals(getResourceSignature(policy.path(RESOURCES)))) {
                return Optional.of(policy);
            }
        }
        return Optional.empty();
    }

    private ObjectNode createResources(final String serviceType, final String resourcePath) {
        final ObjectNode resources = objectMapper.createObjectNode();
        if (OZONE_TYPE.equals(serviceType)) {
            final String[] parts = resourcePath.replaceFirst(LEADING_SEPARATORS, "").split(PATH_SEPARATOR, OZONE_RESOURCE_LEVELS);
            for (int i = 0; i < parts.length; i++) {
                if (parts[i].isEmpty()) {
                    throw new IllegalArgumentException("Ozone resource [%s] not valid".formatted(resourcePath));
                }
                final boolean key = i == OZONE_RESOURCE_LEVELS - 1;
                resources.set(OZONE_RESOURCES.get(i), createResource(parts[i], key));
            }
        } else {
            resources.set(PATH_RESOURCE, createResource(resourcePath, true));
        }
        return resources;
    }

    private ObjectNode createResource(final String value, final boolean recursive) {
        final ObjectNode resource = objectMapper.createObjectNode();
        resource.putArray(VALUES).add(value);
        resource.put(IS_EXCLUDES, false);
        resource.put(IS_RECURSIVE, recursive);
        return resource;
    }

    private ObjectNode createPolicyItem(final String user, final String... accessTypes) {
        final ObjectNode item = objectMapper.createObjectNode();
        item.putArray(USERS).add(user);
        final ArrayNode accesses = item.putArray(ACCESSES);
        for (final String accessType : accessTypes) {
            final ObjectNode access = accesses.addObject();
            access.put(TYPE, accessType);
            access.put("isAllowed", true);
        }
        item.put("delegateAdmin", false);
        return item;
    }

    private int getCurrentPlugins(final String serviceName) throws IOException, InterruptedException {
        final long version = request(GET, SERVICE_BY_NAME_PATH_FORMAT.formatted(encode(serviceName)), null).path(POLICY_VERSION).asLong(UNKNOWN_VERSION);
        final JsonNode plugins = request(GET, PLUGINS_INFO_PATH_FORMAT.formatted(encode(serviceName)), null).path(PLUGIN_INFO_LIST);
        int current = 0;
        for (final JsonNode plugin : plugins) {
            if (!serviceName.equals(plugin.path(SERVICE_NAME).asText())) {
                continue;
            }
            final long activeVersion = plugin.path(INFO).path(POLICY_ACTIVE_VERSION).asLong(UNKNOWN_VERSION);
            if (activeVersion < version) {
                throw new IllegalStateException("Ranger plugin [%s] on [%s] for [%s] active policy version [%d] expected [%d]".formatted(
                        plugin.path(APP_TYPE).asText(), plugin.path(HOST_NAME).asText(), serviceName, activeVersion, version));
            }
            current++;
        }
        return current;
    }

    private JsonNode request(final String method, final String path, final JsonNode body) throws IOException, InterruptedException {
        final HttpResponse<String> response = send(method, path, body);
        if (!isSuccessful(response.statusCode())) {
            throw new IOException(REQUEST_FAILED_FORMAT.formatted(method, path, response.statusCode(), response.body()));
        }
        final String content = response.body();
        return content == null || content.isBlank() ? objectMapper.nullNode() : objectMapper.readTree(content);
    }

    private HttpResponse<String> send(final String method, final String path, final JsonNode body) throws IOException, InterruptedException {
        final HttpRequest.BodyPublisher publisher = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(body), StandardCharsets.UTF_8);
        final HttpRequest request = HttpRequest.newBuilder(URI.create(BASE_URL + path))
                .timeout(REQUEST_TIMEOUT)
                .header(AUTHORIZATION_HEADER, AUTHORIZATION)
                .header(ACCEPT_HEADER, JSON_CONTENT_TYPE)
                .header(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE)
                .method(method, publisher)
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private void logDiagnostics() {
        try {
            final ExecResult result = admin.execInContainer(SHELL, SHELL_COMMAND_OPTION, DIAGNOSTICS_COMMAND);
            LOGGER.warn("Ranger Admin logs:\n{}", result.getStdout());
        } catch (final IOException | RuntimeException e) {
            LOGGER.warn("Reading Ranger Admin logs failed", e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Reading Ranger Admin logs interrupted", e);
        }
    }

    private static String getResourceSignature(final JsonNode resources) {
        final Map<String, String> signature = new TreeMap<>();
        for (final Map.Entry<String, JsonNode> field : resources.properties()) {
            final List<String> values = new ArrayList<>();
            field.getValue().path(VALUES).forEach(value -> values.add(value.asText()));
            values.sort(String::compareTo);
            final JsonNode resource = field.getValue();
            signature.put(field.getKey(), "%s|%s|%s".formatted(values, resource.path(IS_EXCLUDES).asBoolean(false), resource.path(IS_RECURSIVE).asBoolean(false)));
        }
        return signature.toString();
    }

    private static boolean containsText(final JsonNode array, final String text) {
        for (final JsonNode element : array) {
            if (text.equals(element.asText())) {
                return true;
            }
        }
        return false;
    }

    private static boolean isSuccessful(final int statusCode) {
        return statusCode >= HTTP_OK && statusCode < HTTP_MULTIPLE_CHOICES;
    }

    private static String encode(final String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
