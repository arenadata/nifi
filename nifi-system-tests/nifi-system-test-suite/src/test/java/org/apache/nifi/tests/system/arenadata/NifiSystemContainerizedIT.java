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

import org.apache.nifi.tests.system.NiFiClientUtil;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientConfig;
import org.apache.nifi.toolkit.client.impl.JerseyNiFiClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class NifiSystemContainerizedIT extends NiFiSystemIT {
    private static final Logger logger = LoggerFactory.getLogger(NifiSystemContainerizedIT.class);

    @Override
    @BeforeEach
    public void setup(final TestInfo testInfo) throws IOException, NoSuchAlgorithmException {
        super.testInfo = testInfo;
        final String testClassName = testInfo.getTestClass().map(Class::getSimpleName).orElse("<Unknown Test Class>");
        final String friendlyTestName = testClassName + ":" + testInfo.getDisplayName();
        logger.info("Beginning Test {}", friendlyTestName);

        Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
        setupClient(9091);
    }

    protected void setupClient(final int apiPort) {
        nifiClient = createClient(apiPort);
        clientUtil = new NiFiClientUtil(nifiClient, getNiFiVersion(), getTestName());
    }

    protected NiFiClient createClient(final int port) {
        final NiFiClientConfig.Builder clientConfigBuilder = new NiFiClientConfig.Builder()
                .baseUrl("http://localhost:" + port)
                .connectTimeout(15000)
                .readTimeout(30000);

        return new JerseyNiFiClient.Builder()
                .config(clientConfigBuilder.build())
                .build();
    }
}
