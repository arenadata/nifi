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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.metrics.LoggingMetricsReporter;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ClassloaderIsolationKeyProvider;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.services.iceberg.IcebergCatalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SKIPPED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;

@SupportsSensitiveDynamicProperties
@RequiresInstanceClassLoading
@Tags({"iceberg", "catalog", "hive", "metastore", "hdfs", "ozone"})
@CapabilityDescription("Provides Apache Iceberg integration with catalogs managed in Apache Hive Metastore using Hadoop File IO for Data Files")
public class HiveMetastoreIcebergCatalog extends AbstractControllerService implements IcebergCatalog, VerifiableControllerService, ClassloaderIsolationKeyProvider {

    static final PropertyDescriptor METASTORE_URI = new PropertyDescriptor.Builder()
            .name("Hive Metastore URI")
            .description("Comma-separated list of Apache Hive Metastore Thrift URIs, overriding the metastore.thrift.uris and hive.metastore.uris properties from Hadoop Configuration Resources")
            .required(false)
            .addValidator(StandardValidators.URI_LIST_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor WAREHOUSE_LOCATION = new PropertyDescriptor.Builder()
            .name("Warehouse Location")
            .description("Default location of the Catalog warehouse overriding the hive.metastore.warehouse.dir property")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor HADOOP_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("Hadoop Configuration Resources")
            .description("A file or comma-separated list of files containing Hadoop and Hive configuration such as core-site.xml, hdfs-site.xml and hive-site.xml")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor ADDITIONAL_CLASSPATH_RESOURCES = new PropertyDescriptor.Builder()
            .name("Additional Classpath Resources")
            .description("A comma-separated list of files and directories added to the classpath, supporting File System implementations that are not bundled, such as Apache Ozone")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY)
            .dynamicallyModifiesClasspath(true)
            .build();

    static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("Kerberos User Service")
            .description("Kerberos User Controller Service providing the principal and keytab for authenticating with the Hive Metastore and the File System")
            .required(false)
            .identifiesControllerService(KerberosUserService.class)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            METASTORE_URI,
            WAREHOUSE_LOCATION,
            HADOOP_CONFIGURATION_RESOURCES,
            ADDITIONAL_CLASSPATH_RESOURCES,
            KERBEROS_USER_SERVICE
    );

    private static final String METASTORE_URIS_PROPERTY = "hive.metastore.uris";

    private static final String METASTORE_THRIFT_URIS_PROPERTY = "metastore.thrift.uris";

    private static final String CLIENT_SOCKET_TIMEOUT_PROPERTY = "hive.metastore.client.socket.timeout";

    private static final String CLIENT_SOCKET_TIMEOUT_DEFAULT = "60s";

    private static final String CLIENT_POOL_CACHE_KEY_PROPERTY = "nifi.iceberg.catalog.client-pool-key";

    private static final String CLIENT_POOL_CACHE_KEY_ELEMENT = "conf:" + CLIENT_POOL_CACHE_KEY_PROPERTY;

    private static final String CLIENT_POOL_CACHE_KEY_FORMAT = "%s-%s";

    private static final String CLIENT_POOL_CACHE_KEY_ELEMENTS_FORMAT = "%s," + CLIENT_POOL_CACHE_KEY_ELEMENT;

    private static final String CONFIGURATION_STEP = "Catalog Configuration";

    private static final String CONNECTION_STEP = "Metastore Connection";

    private static final String INITIALIZED_STATUS = "Initialized";

    private static final String NAMESPACES_FOUND = "Namespaces found [%d]";

    private static final String OZONE_FILE_SYSTEM_PROPERTY = "fs.ofs.impl";

    private static final String OZONE_FILE_SYSTEM_CLASS = "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem";

    private static final String METASTORE_URI_NOT_FOUND = "Hive Metastore URI not found: configure the property or provide a configuration file containing %s or %s";

    private static final String CONFIGURATION_FAILED = "Catalog Configuration failed";

    private static final String SPACE_SEPARATOR = " ";

    private static final String CAUSE_SEPARATOR = ": caused by ";

    private static final String URI_SEPARATOR = ",";

    private static final int MAXIMUM_CAUSE_DEPTH = 5;

    private volatile HiveCatalog catalog;

    private volatile Catalog providedCatalog;

    private volatile KerberosUser kerberosUser;

    private volatile UserGroupInformation userGroupInformation;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyName) {
        return new PropertyDescriptor.Builder()
                .name(propertyName)
                .description("Apache Iceberg Catalog property passed to the Hive Catalog")
                .dynamic(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        try {
            if (!isMetastoreUriConfigured(validationContext)) {
                results.add(new ValidationResult.Builder()
                        .subject(METASTORE_URI.getName())
                        .valid(false)
                        .explanation(METASTORE_URI_NOT_FOUND.formatted(METASTORE_THRIFT_URIS_PROPERTY, METASTORE_URIS_PROPERTY))
                        .build()
                );
            }
        } catch (final Throwable e) {
            results.add(new ValidationResult.Builder()
                    .subject(HADOOP_CONFIGURATION_RESOURCES.getName())
                    .valid(false)
                    .explanation("Hadoop Configuration Resources not read: %s".formatted(getExplanation(e)))
                    .build()
            );
        }

        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final Configuration configuration = getHadoopConfiguration(context);

        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
        try {
            if (kerberosUserService == null) {
                kerberosUser = null;
                userGroupInformation = null;
            } else {
                kerberosUser = kerberosUserService.createKerberosUser();
                userGroupInformation = SecurityUtil.getUgiForKerberosUser(configuration, kerberosUser);
            }

            catalog = getInitializedCatalog(context, configuration, userGroupInformation);
            providedCatalog = ContextClassLoaderInvocationHandler.getProxy(Catalog.class, catalog, getClass().getClassLoader());
        } catch (final IOException | RuntimeException e) {
            close(catalog);
            catalog = null;
            providedCatalog = null;
            logout(kerberosUser);
            kerberosUser = null;
            userGroupInformation = null;
            throw new ProcessException("Hive Catalog initialization failed", e);
        }
    }

    @OnDisabled
    public void onDisabled() {
        close(catalog);
        catalog = null;
        providedCatalog = null;

        logout(kerberosUser);
        kerberosUser = null;
        userGroupInformation = null;
    }

    @Override
    public Catalog getCatalog() {
        return providedCatalog;
    }

    HiveCatalog getHiveCatalog() {
        return catalog;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog componentLog, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        KerberosUser verificationUser = null;
        HiveCatalog verificationCatalog = null;
        try {
            final Configuration configuration = getHadoopConfiguration(context);

            UserGroupInformation verificationUgi = null;
            final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
            if (kerberosUserService != null) {
                verificationUser = kerberosUserService.createKerberosUser();
                verificationUgi = SecurityUtil.getUgiForKerberosUser(configuration, verificationUser);
            }

            verificationCatalog = getInitializedCatalog(context, configuration, verificationUgi);
            componentLog.info("Hive Catalog Initialized [{}]", verificationCatalog.name());
            results.add(getSuccessfulResult(CONFIGURATION_STEP, INITIALIZED_STATUS));

            final HiveCatalog initializedCatalog = verificationCatalog;
            final UserGroupInformation ugi = verificationUgi;
            try {
                final List<Namespace> namespaces = SecurityUtil.callWithUgi(ugi, () -> initializedCatalog.listNamespaces(Namespace.empty()));
                results.add(getSuccessfulResult(CONNECTION_STEP, NAMESPACES_FOUND.formatted(namespaces.size())));
            } catch (final Throwable e) {
                componentLog.warn("Hive Metastore connection failed", e);
                results.add(getFailedResult(CONNECTION_STEP, e));
            }
        } catch (final Throwable e) {
            componentLog.warn("Hive Catalog configuration failed", e);
            results.add(getFailedResult(CONFIGURATION_STEP, e));
            results.add(getSkippedResult(CONNECTION_STEP, CONFIGURATION_FAILED));
        } finally {
            close(verificationCatalog);
            logout(verificationUser);
        }

        return results;
    }

    @Override
    public String getClassloaderIsolationKey(final PropertyContext context) {
        try {
            final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
            if (kerberosUserService != null) {
                final KerberosUser isolationUser = kerberosUserService.createKerberosUser();
                return isolationUser.getPrincipal();
            }
        } catch (final IllegalStateException e) {
            getLogger().debug("Kerberos User Service not available for Classloader Isolation Key", e);
        }

        return null;
    }

    private HiveCatalog getInitializedCatalog(final ConfigurationContext context, final Configuration configuration, final UserGroupInformation ugi) throws IOException {
        final Map<String, String> properties = new HashMap<>();

        properties.put(CatalogProperties.METRICS_REPORTER_IMPL, LoggingMetricsReporter.class.getName());

        properties.putAll(getDynamicProperties(context));

        configuration.set(CLIENT_POOL_CACHE_KEY_PROPERTY, CLIENT_POOL_CACHE_KEY_FORMAT.formatted(getIdentifier(), UUID.randomUUID()));

        final String configuredCacheKeys = properties.get(CatalogProperties.CLIENT_POOL_CACHE_KEYS);
        final String cacheKeys = configuredCacheKeys == null || configuredCacheKeys.isBlank()
                ? CLIENT_POOL_CACHE_KEY_ELEMENT
                : CLIENT_POOL_CACHE_KEY_ELEMENTS_FORMAT.formatted(configuredCacheKeys.trim());
        properties.put(CatalogProperties.CLIENT_POOL_CACHE_KEYS, cacheKeys);

        final String metastoreUri = getNormalizedUriList(context.getProperty(METASTORE_URI).evaluateAttributeExpressions().getValue());
        if (metastoreUri != null) {
            properties.put(CatalogProperties.URI, metastoreUri);
            configuration.set(METASTORE_THRIFT_URIS_PROPERTY, metastoreUri);
        }

        final String warehouseLocation = context.getProperty(WAREHOUSE_LOCATION).evaluateAttributeExpressions().getValue();
        if (warehouseLocation != null && !warehouseLocation.isBlank()) {
            properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation.trim());
        }

        final String identifier = getIdentifier();

        return SecurityUtil.callWithUgi(ugi, () -> {
            final HiveCatalog hiveCatalog = new HiveCatalog();
            hiveCatalog.setConf(configuration);
            hiveCatalog.initialize(identifier, properties);
            return hiveCatalog;
        });
    }

    private Configuration getHadoopConfiguration(final PropertyContext context) {
        final Configuration configuration = new Configuration();

        final ResourceReferences configurationResources = context.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().asResources();
        for (final String location : configurationResources.asLocations()) {
            configuration.addResource(new Path(location));
        }

        configuration.setIfUnset(CLIENT_SOCKET_TIMEOUT_PROPERTY, CLIENT_SOCKET_TIMEOUT_DEFAULT);
        configuration.setIfUnset(OZONE_FILE_SYSTEM_PROPERTY, OZONE_FILE_SYSTEM_CLASS);

        return configuration;
    }

    private boolean isMetastoreUriConfigured(final ValidationContext validationContext) {
        final String metastoreUri = getNormalizedUriList(validationContext.getProperty(METASTORE_URI).evaluateAttributeExpressions().getValue());
        if (metastoreUri != null) {
            return true;
        }

        if (!validationContext.getProperty(HADOOP_CONFIGURATION_RESOURCES).isSet()) {
            return false;
        }

        final Configuration configuration = getHadoopConfiguration(validationContext);
        final String configuredUris = configuration.get(METASTORE_THRIFT_URIS_PROPERTY, configuration.get(METASTORE_URIS_PROPERTY));
        return configuredUris != null && !configuredUris.isBlank();
    }

    private String getNormalizedUriList(final String uriList) {
        if (uriList == null || uriList.isBlank()) {
            return null;
        }

        final String normalized = Arrays.stream(uriList.split(URI_SEPARATOR))
                .map(String::trim)
                .filter(uri -> !uri.isEmpty())
                .collect(Collectors.joining(URI_SEPARATOR));

        return normalized.isEmpty() ? null : normalized;
    }

    private Map<String, String> getDynamicProperties(final ConfigurationContext context) {
        final Map<String, String> properties = new HashMap<>();

        for (final Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = property.getKey();
            if (descriptor.isDynamic()) {
                properties.put(descriptor.getName(), property.getValue());
            }
        }

        return properties;
    }

    private ConfigVerificationResult getSuccessfulResult(final String step, final String explanation) {
        return new ConfigVerificationResult.Builder()
                .verificationStepName(step)
                .outcome(SUCCESSFUL)
                .explanation(explanation)
                .build();
    }

    private ConfigVerificationResult getFailedResult(final String step, final Throwable e) {
        return new ConfigVerificationResult.Builder()
                .verificationStepName(step)
                .outcome(FAILED)
                .explanation(getExplanation(e))
                .build();
    }

    private ConfigVerificationResult getSkippedResult(final String step, final String explanation) {
        return new ConfigVerificationResult.Builder()
                .verificationStepName(step)
                .outcome(SKIPPED)
                .explanation(explanation)
                .build();
    }

    private String getExplanation(final Throwable e) {
        final StringBuilder explanation = new StringBuilder();
        appendThrowable(explanation, e);

        Throwable cause = e.getCause();
        for (int depth = 0; cause != null && depth < MAXIMUM_CAUSE_DEPTH; depth++) {
            explanation.append(CAUSE_SEPARATOR);
            appendThrowable(explanation, cause);
            cause = cause.getCause() == cause ? null : cause.getCause();
        }

        return explanation.toString();
    }

    private void appendThrowable(final StringBuilder explanation, final Throwable e) {
        explanation.append(e.getClass().getSimpleName());

        final String message = e.getMessage();
        if (message != null && !message.isBlank()) {
            explanation.append(SPACE_SEPARATOR);
            explanation.append(message);
        }
    }

    private void close(final HiveCatalog closeableCatalog) {
        if (closeableCatalog != null) {
            try {
                closeableCatalog.close();
            } catch (final IOException e) {
                getLogger().warn("Close Catalog failed", e);
            }
        }
    }

    private void logout(final KerberosUser user) {
        if (user != null) {
            try {
                user.logout();
            } catch (final Exception e) {
                getLogger().warn("Kerberos User logout failed", e);
            }
        }
    }
}
