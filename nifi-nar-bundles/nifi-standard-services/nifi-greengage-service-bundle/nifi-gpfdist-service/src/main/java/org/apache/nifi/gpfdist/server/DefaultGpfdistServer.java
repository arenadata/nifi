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
package org.apache.nifi.gpfdist.server;

import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextManager;
import org.apache.nifi.gpfdist.metadata.GpfidstLoadConfig;
import org.apache.nifi.gpfdist.server.config.GpfdistServerConfig;
import org.apache.nifi.gpfdist.server.servlet.GpfdistAsyncServlet;
import org.apache.nifi.gpfdist.server.servlet.GpfdistServletContextListener;
import org.apache.nifi.gpfdist.service.load.process.RecordProcessorFactory;
import org.apache.nifi.gpfdist.service.unload.process.InputDataProcessorFactory;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.StringUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import java.util.concurrent.ExecutorService;

public class DefaultGpfdistServer implements GpfdistServer {
    private static final String GPFDIST_READ_ENDPOINT = "/read/*";
    private static final String GPFDIST_WRITE_ENDPOINT = "/write/*";
    private static final String GPFDIST_CONTEXT_PATH = "/gpfdist";
    private final GpfdistServerConfig serverConfig;
    private volatile JettyServer server;
    private final ContextManager<Context> writeContextManager;
    private final ContextManager<Context> readContextManager;
    private final InputDataProcessorFactory inputDataProcessorFactory;
    private final RecordProcessorFactory recordProcessorFactory;
    private final ExecutorService gpfdistRequestProcessingExecutorService;
    private final GpfidstLoadConfig gpfidstLoadConfig;
    private final ComponentLog logger;

    public DefaultGpfdistServer(final GpfdistServerConfig serverConfig,
                                final ContextManager<Context> writeContextManager,
                                final ContextManager<Context> readContextManager,
                                final InputDataProcessorFactory inputDataProcessorFactory,
                                final RecordProcessorFactory recordProcessorFactory,
                                final ExecutorService gpfdistRequestProcessingExecutorService,
                                final GpfidstLoadConfig gpfidstLoadConfig,
                                ComponentLog logger) {
        this.serverConfig = serverConfig;
        this.writeContextManager = writeContextManager;
        this.readContextManager = readContextManager;
        this.inputDataProcessorFactory = inputDataProcessorFactory;
        this.gpfdistRequestProcessingExecutorService = gpfdistRequestProcessingExecutorService;
        this.gpfidstLoadConfig = gpfidstLoadConfig;
        this.logger = logger;
        this.recordProcessorFactory = recordProcessorFactory;
    }

    @Override
    public void start() {
        try {
            server = new JettyServer(serverConfig,
                    writeContextManager,
                    readContextManager,
                    inputDataProcessorFactory,
                    recordProcessorFactory,
                    gpfdistRequestProcessingExecutorService,
                    gpfidstLoadConfig,
                    logger);
            server.start();
        } catch (Exception e) {
            logger.error("Failed to start gpfdist server", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                logger.error("Failed to stop gpfdist server", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public int getPort() {
        return server.getPort();
    }

    @Override
    public String getHost() {
        return server.getHost();
    }

    @Override
    public boolean isRunning() {
        return server.isRunning();
    }

    public static class JettyServer {
        private final GpfdistServerConfig serverConfig;
        private final ContextManager<Context> writeContextManager;
        private final ContextManager<Context> readContextManager;
        private final InputDataProcessorFactory inputDataProcessorFactory;
        private final RecordProcessorFactory recordProcessorFactory;
        private final ExecutorService gpfdistRequestExecutorService;
        private final GpfidstLoadConfig gpfidstLoadConfig;
        private final ComponentLog logger;
        private ServerConnector connector;
        private Server server;

        public JettyServer(final GpfdistServerConfig serverConfig,
                           final ContextManager<Context> writeContextManager,
                           final ContextManager<Context> readContextManager,
                           final InputDataProcessorFactory inputDataProcessorFactory,
                           final RecordProcessorFactory recordProcessorFactory,
                           final ExecutorService gpfdistRequestExecutorService,
                           final GpfidstLoadConfig gpfidstLoadConfig,
                           ComponentLog logger) {
            this.writeContextManager = writeContextManager;
            this.readContextManager = readContextManager;
            this.inputDataProcessorFactory = inputDataProcessorFactory;
            this.recordProcessorFactory = recordProcessorFactory;
            this.gpfdistRequestExecutorService = gpfdistRequestExecutorService;
            this.gpfidstLoadConfig = gpfidstLoadConfig;
            this.logger = logger;
            this.serverConfig = serverConfig;
        }

        public void start() throws Exception {
            server = new Server(new QueuedThreadPool(serverConfig.getMaxThreads(),
                    serverConfig.getMinThreads(),
                    serverConfig.getIdleTimeoutMs()));
            connector = new ServerConnector(server);
            connector.setIdleTimeout(serverConfig.getIdleTimeoutMs());
            connector.setPort(serverConfig.getPort());
            if (StringUtils.isNotBlank(serverConfig.getHost())) {
                connector.setHost(serverConfig.getHost());
            }
            server.setConnectors(new Connector[]{connector});
            server.setHandler(createServletContextHandler());
            server.start();
        }

        private ServletContextHandler createServletContextHandler() {
            ServletContextHandler context = new ServletContextHandler();
            context.setContextPath(GPFDIST_CONTEXT_PATH);
            context.addEventListener(new GpfdistServletContextListener(writeContextManager,
                    readContextManager,
                    recordProcessorFactory,
                    inputDataProcessorFactory,
                    gpfdistRequestExecutorService,
                    gpfidstLoadConfig,
                    logger));
            context.addServlet(GpfdistAsyncServlet.class, GPFDIST_READ_ENDPOINT);
            context.addServlet(GpfdistAsyncServlet.class, GPFDIST_WRITE_ENDPOINT);
            return context;
        }

        public void stop() throws Exception {
            server.stop();
        }

        public int getPort() {
            return connector.getPort();
        }

        public String getHost() {
            return connector.getHost();
        }

        public boolean isRunning() {
            return server.isRunning();
        }
    }

    @Override
    public String toString() {
        return "DefaultGpfdistServer{" +
                "host=" + server.getHost() +
                ", port=" + server.getPort() +
                '}';
    }
}
