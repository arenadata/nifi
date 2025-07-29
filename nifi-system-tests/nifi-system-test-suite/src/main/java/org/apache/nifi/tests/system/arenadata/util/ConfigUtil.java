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
