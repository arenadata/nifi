package org.apache.nifi.tests.system.arenadata.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.SneakyThrows;
import org.apache.nifi.tests.system.arenadata.model.TestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

public class ConfigUtil {

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtil.class);
    private static TestConfig testConfig;

    public static TestConfig getTestConfig() {
        return testConfig == null ? createTestConfig() : testConfig;
    }

    @SneakyThrows
    private static TestConfig createTestConfig() {
        String configFile = "arenadata/it-config.yaml";
        URL url = ConfigUtil.class.getClassLoader().getResource(configFile);
        testConfig = mapper.readValue(url, TestConfig.class);
        logger.info("Initialized config {} from file {}", testConfig, configFile);
        return testConfig;
    }
}
