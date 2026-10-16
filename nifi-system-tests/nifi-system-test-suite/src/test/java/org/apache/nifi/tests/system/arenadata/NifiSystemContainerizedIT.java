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
package org.apache.nifi.tests.system.arenadata;

import io.qameta.allure.Step;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.nifi.registry.security.util.KeystoreType;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.arenadata.model.Component;
import org.apache.nifi.tests.system.arenadata.service.DockerComposeService;
import org.apache.nifi.tests.system.arenadata.service.JdbcService;
import org.apache.nifi.tests.system.arenadata.service.JdbcServiceFactory;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientConfig;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.impl.JerseyNiFiClient;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.function.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.nifi.tests.system.arenadata.util.ConfigUtil.getTestConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class NifiSystemContainerizedIT extends NiFiSystemIT {
    private static final Logger logger = LoggerFactory.getLogger(NifiSystemContainerizedIT.class);
    private static final String NIFI_KEYSTORE_RELATIVE_PATH = "env/arenadata/nifi-certs/nifi01-keystore.p12";
    private static final String NIFI_KEYSTORE_PROJECT_RELATIVE_PATH = "nifi-system-tests/nifi-system-test-suite/" + NIFI_KEYSTORE_RELATIVE_PATH;
    private static final String NIFI_KEYSTORE_PASSWORD = "changeit";
    private static final String NIFI_TRUSTSTORE_RELATIVE_PATH = "env/arenadata/nifi-certs/truststore.p12";
    private static final String NIFI_TRUSTSTORE_PROJECT_RELATIVE_PATH = "nifi-system-tests/nifi-system-test-suite/" + NIFI_TRUSTSTORE_RELATIVE_PATH;
    private static final String NIFI_TRUSTSTORE_PASSWORD = "changeit";
    private static final int NIFI_CLUSTER_NODE_COUNT = 3;
    protected static JdbcService adbService;
    protected static JdbcService postgresService;
    protected static final String CREATE_ENUM_SQL = "DO $$ \n" +
            "BEGIN\n" +
            "    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'day') THEN\n" +
            "        CREATE TYPE day AS ENUM ('sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat');\n" +
            "    END IF;\n" +
            "END $$;";

    @BeforeAll
    public static void setup() {
        if (adbService == null && postgresService == null) {
            DockerComposeService composeService = new DockerComposeService(List.of(Component.values()));
            composeService.init();
            JdbcServiceFactory jdbcServiceFactory = new JdbcServiceFactory();
            adbService = jdbcServiceFactory.adbService();
            postgresService = jdbcServiceFactory.postgresService();
        }
    }

    @Override
    @BeforeEach
    public void setup(final TestInfo testInfo) {
        super.testInfo = testInfo;
        final String testClassName = testInfo.getTestClass().map(Class::getSimpleName).orElse("<Unknown Test Class>");
        final String friendlyTestName = testClassName + ":" + testInfo.getDisplayName();
        logger.info("Beginning Test {}", friendlyTestName);

        Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
        setupClient();
        waitForAllNodesConnected(NIFI_CLUSTER_NODE_COUNT);
    }

    @Override
    protected void destroyFlow() throws NiFiClientException, IOException, InterruptedException {
        waitForAllNodesConnected(NIFI_CLUSTER_NODE_COUNT);
        super.destroyFlow();
    }

    @Override
    protected NiFiClient createClient(final int port) {
        final String baseUrl = "https://localhost:" + port;

        final NiFiClientConfig.Builder clientConfigBuilder = new NiFiClientConfig.Builder()
                .baseUrl(baseUrl)
                .connectTimeout(15000)
                .readTimeout(30000)
                .keystoreFilename(getNifiKeystorePath())
                .keystorePassword(NIFI_KEYSTORE_PASSWORD)
                .keyPassword(NIFI_KEYSTORE_PASSWORD)
                .keystoreType(KeystoreType.PKCS12)
                .truststoreFilename(getNifiTruststorePath())
                .truststorePassword(NIFI_TRUSTSTORE_PASSWORD)
                .truststoreType(KeystoreType.PKCS12);

        return new JerseyNiFiClient.Builder()
                .config(clientConfigBuilder.build())
                .build();
    }

    private String getNifiKeystorePath() {
        return getNifiCertificateStorePath(NIFI_KEYSTORE_RELATIVE_PATH, NIFI_KEYSTORE_PROJECT_RELATIVE_PATH, "keystore");
    }

    private String getNifiTruststorePath() {
        return getNifiCertificateStorePath(NIFI_TRUSTSTORE_RELATIVE_PATH, NIFI_TRUSTSTORE_PROJECT_RELATIVE_PATH, "truststore");
    }

    private String getNifiCertificateStorePath(final String moduleRelativePath, final String projectRelativePath, final String storeType) {
        final Path workingDirectory = Paths.get(System.getProperty("user.dir"));
        final Path moduleStorePath = workingDirectory.resolve(moduleRelativePath);
        if (Files.isRegularFile(moduleStorePath)) {
            return moduleStorePath.toAbsolutePath().normalize().toString();
        }
        final Path projectStorePath = workingDirectory.resolve(projectRelativePath);
        if (Files.isRegularFile(projectStorePath)) {
            return projectStorePath.toAbsolutePath().normalize().toString();
        }
        throw new IllegalStateException(String.format(
                "NiFi %s not found. Checked [%s] and [%s]. Generate Arenadata NiFi certificates before running containerized system tests.",
                storeType,
                moduleStorePath.toAbsolutePath().normalize(),
                projectStorePath.toAbsolutePath().normalize()));
    }

    @Override
    protected int getClientApiPort() {
        return getTestConfig().getClientApiPort();
    }

    @SneakyThrows
    protected void enableControllerServiceAndWait(ControllerServiceEntity controllerServiceEntity) {
        getClientUtil().enableControllerService(controllerServiceEntity);
        getClientUtil().waitForControllerServicesEnabled(controllerServiceEntity.getParentGroupId(), controllerServiceEntity.getId());
    }

    @Step("Assert with polling")
    protected void assertWithPooling(Executable assertion) {
        Awaitility.waitAtMost(Duration.ofSeconds(getTestConfig().getGeneralTimeout()))
                .pollInterval(Duration.ofSeconds(getTestConfig().getPollInterval()))
                .untilAsserted(assertion::execute);
    }

    @Step("Assert error message")
    protected void assertErrorMessage(ProcessorEntity processor, String expected) {
        assertWithPooling(() -> {
            val processorInfo = getNifiClient().getProcessorClient().getProcessor(processor.getId());
            assertEquals(processorInfo
                            .getBulletins().stream().filter(b -> b.getBulletin().getLevel().equals("ERROR")).findFirst().orElseThrow()
                            .getBulletin().getMessage().contains(expected),
                    true,
                    () -> String.format("Processor bulletins {%s} don`t contains ERROR with text '%s'",
                            processorInfo.getBulletins().stream()
                                    .map(bulletin -> bulletin.getBulletin().getMessage()).collect(Collectors.joining()),
                            expected));
        });
    }

    protected String getFieldsString(Map<String, String> fieldMap) {
        return fieldMap.entrySet().stream()
                .map(entry -> String.format("%s %s", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(", "));
    }

    protected String getFieldNamesString(Map<String, String> fieldMap) {
        return fieldMap.entrySet().stream()
                .map(Map.Entry::getKey)
                .collect(Collectors.joining(", "));
    }
}
