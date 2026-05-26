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
package org.apache.nifi.gpfdist.service;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestStandardGpfdistService {
    private static final String GPFDIST_SERVICE_ID = StandardGpfdistService.class.getName();
    private static final String DBCP_SERVICE_ID = MockDBCPService.class.getName();
    private TestRunner runner;
    private StandardGpfdistService service;

    @BeforeEach
    void setUp() throws InitializationException {
        service = new StandardGpfdistService();
        MockDBCPService mockDBCPService = new MockDBCPService();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);

        runner.addControllerService(DBCP_SERVICE_ID, mockDBCPService);
        runner.addControllerService(GPFDIST_SERVICE_ID, service);
        runner.enableControllerService(mockDBCPService);

        runner.setProperty(service, GpfdistProperties.PORT, getRandomFreePort());
        runner.setProperty(service, GpfdistProperties.DBCP_SERVICE, DBCP_SERVICE_ID);
    }

    @Test
    void testPortInvalid() {
        runner.setProperty(service, GpfdistProperties.PORT, "-1");
        runner.assertNotValid(service);
    }

    @Test
    void testGpfdistServerMaxThreadsInvalid() {
        runner.setProperty(service, GpfdistProperties.GPFDIST_SERVER_MAX_THREADS, "-1");
        runner.assertNotValid(service);
    }

    @Test
    void testGpfdistServerMinThreadsInvalid() {
        runner.setProperty(service, GpfdistProperties.GPFDIST_SERVER_MIN_THREADS, "-1");
        runner.assertNotValid(service);
    }

    @Test
    void testGpfdistServerThreadIdleTimeoutInvalid() {
        runner.setProperty(service, GpfdistProperties.GPFDIST_SERVER_THREAD_IDLE_TIMEOUT_MS, "invalidTimeout");
        runner.assertNotValid(service);
    }

    @Test
    void testServiceMethods() {
        runner.enableControllerService(service);
        assertNotNull(service.getGreengageMetadataService());
        assertNotNull(service.getCreateReadExternalTableQueryExecutor());
        runner.disableControllerService(service);
    }

    private String getRandomFreePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return String.valueOf(serverSocket.getLocalPort());
        } catch (IOException e) {
            throw new RuntimeException("Port is not available");
        }
    }

    private static class MockDBCPService extends AbstractControllerService implements DBCPService {
        @Override
        public Connection getConnection() throws ProcessException {
            return null;
        }
    }
}
