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

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextManager;
import org.apache.nifi.gpfdist.server.DefaultGpfdistServer;
import org.apache.nifi.gpfdist.server.GpfdistServer;
import org.apache.nifi.gpfdist.server.config.GpfdistServerConfig;
import org.apache.nifi.gpfdist.service.cluster.ClusterStateNodeIndexService;
import org.apache.nifi.gpfdist.service.context.DefaultContextManager;
import org.apache.nifi.gpfdist.service.greengage.DefaultGreengageService;
import org.apache.nifi.gpfdist.service.load.metadata.factory.CreateReadableExternalTableQueryFactory;
import org.apache.nifi.gpfdist.service.load.metadata.factory.DefaulGpfdistLocationFactory;
import org.apache.nifi.gpfdist.service.load.metadata.factory.DefaultGpfdistLoadMetadataFactory;
import org.apache.nifi.gpfdist.service.load.metadata.factory.DefaultInsertDataQueryFactory;
import org.apache.nifi.gpfdist.service.load.process.GpfdistRecordProcessorFactory;
import org.apache.nifi.gpfdist.service.load.process.GpfdistRecordSinkProvider;
import org.apache.nifi.gpfdist.service.load.process.RecordProcessorFactory;
import org.apache.nifi.gpfdist.service.load.query.LoadDataQueryExecutor;
import org.apache.nifi.gpfdist.service.metadata.CsvFormatConfig;
import org.apache.nifi.gpfdist.service.metadata.DefaultExternalTableFormatConfigFactory;
import org.apache.nifi.gpfdist.service.unload.metadata.DefaultGpfdistUnloadMetadataFactory;
import org.apache.nifi.gpfdist.service.unload.process.GpfdistInputDataProcessorFactory;
import org.apache.nifi.gpfdist.service.unload.process.InputDataProcessorFactory;
import org.apache.nifi.gpfdist.service.unload.query.CreateWritableExternalTableQueryFactory;
import org.apache.nifi.gpfdist.service.unload.query.UnloadDataQueryExecutor;
import org.apache.nifi.gpfdist.service.unload.query.UnloadInsertDataQueryFactory;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.nifi.gpfdist.service.GpfdistProperties.DBCP_SERVICE;
import static org.apache.nifi.gpfdist.service.GpfdistProperties.GPFDIST_REQUEST_PROCESSOR_MAX_THREADS;
import static org.apache.nifi.gpfdist.service.GpfdistProperties.GPFDIST_SERVER_MAX_THREADS;
import static org.apache.nifi.gpfdist.service.GpfdistProperties.GPFDIST_SERVER_MIN_THREADS;
import static org.apache.nifi.gpfdist.service.GpfdistProperties.GPFDIST_SERVER_THREAD_IDLE_TIMEOUT_MS;
import static org.apache.nifi.gpfdist.service.GpfdistProperties.PORT;
import static org.apache.nifi.gpfdist.service.GpfdistProperties.RECORD_PROCESSOR_MAX_THREADS;
import static org.apache.nifi.gpfdist.service.GpfdistProperties.WRITE_BUFFER_SIZE;
import static org.apache.nifi.gpfdist.service.util.ClusterNodeUtil.getNodesHostnames;

@Stateful(description = "Store information about cluster nodes hostnames", scopes = {Scope.CLUSTER})
@Tags({"gpfdist"})
@CapabilityDescription("Provides the ability to load data to Greengage segments directly")
public class StandartGpfdistService extends AbstractControllerService implements GpfdistService {
    private static final String KEY_HOSTS = "hosts";
    private static final int MAX_REGISTER_ATTEMPTS = 10;
    private static final List<PropertyDescriptor> PROPERTIES;

    static final PropertyDescriptor NODE_COUNT = new PropertyDescriptor.Builder()
            .name("Total Nifi Nodes")
            .description("Total nifi cluster nodes count")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PORT);
        props.add(DBCP_SERVICE);
        props.add(WRITE_BUFFER_SIZE);
        props.add(RECORD_PROCESSOR_MAX_THREADS);
        props.add(GPFDIST_REQUEST_PROCESSOR_MAX_THREADS);
        props.add(GPFDIST_SERVER_MIN_THREADS);
        props.add(GPFDIST_SERVER_MAX_THREADS);
        props.add(GPFDIST_SERVER_THREAD_IDLE_TIMEOUT_MS);
        props.add(NODE_COUNT);
        PROPERTIES = Collections.unmodifiableList(props);
    }

    private GpfdistServer server;
    private RecordSinkProvider recordSinkProvider;
    private GreengageService greengageService;
    private TransferDataQueryExecutor loadDataQueryExecutor;
    private TransferDataQueryExecutor unloadDataQueryExecutor;
    private GpfdistUnloadMetadataFactory gpfdistUnloadMetadataFactory;
    private ContextManager<Context> readContextManager;
    private NodeIndexService nodeIndexService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) {
        try {
            ComponentLog logger = getLogger();
            if (server != null && server.isRunning()) {
                logger.info("A Gpfdist server is already running. {}", server);
                return;
            }
            final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
            int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
            final String host = getLocalCanonicalHostname();
            int writeBufferSize = context.getProperty(WRITE_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
            int minServerThreads = context.getProperty(GPFDIST_SERVER_MIN_THREADS).asInteger();
            int maxServerThreads = context.getProperty(GPFDIST_SERVER_MAX_THREADS).asInteger();
            int threadsIdleTimeout = context.getProperty(GPFDIST_SERVER_THREAD_IDLE_TIMEOUT_MS).asInteger();
            int recordProcessorMaxThreads = context.getProperty(RECORD_PROCESSOR_MAX_THREADS).asInteger();
            int gpfdistRequestMaxThreads = context.getProperty(GPFDIST_REQUEST_PROCESSOR_MAX_THREADS).asInteger();
            int nodeCount = context.getProperty(NODE_COUNT).asInteger();

            final ExecutorService recordProcessingExecutorService = Executors.newFixedThreadPool(recordProcessorMaxThreads);
            final ExecutorService queryExecutorService = Executors.newCachedThreadPool();
            final ExecutorService requestExecutorService = Executors.newFixedThreadPool(gpfdistRequestMaxThreads);

            final ContextManager<Context> writeContextManager = new DefaultContextManager(logger);
            readContextManager = new DefaultContextManager(logger);
            final CsvFormatConfig dataFormatConfig = new CsvFormatConfig();
            final RecordProcessorFactory recordProcessorFactory = new GpfdistRecordProcessorFactory(dataFormatConfig);
            final GpfdistServerConfig gpfdistServerConfig = new GpfdistServerConfig(port,
                    host,
                    minServerThreads,
                    maxServerThreads,
                    threadsIdleTimeout,
                    false);
            InputDataProcessorFactory inputDataProcessorFactory = new GpfdistInputDataProcessorFactory(dataFormatConfig, logger);
            server = new DefaultGpfdistServer(gpfdistServerConfig,
                    writeContextManager,
                    readContextManager,
                    inputDataProcessorFactory,
                    recordProcessorFactory,
                    requestExecutorService,
                    logger);
            server.start();
            greengageService = new DefaultGreengageService(dbcpService, logger);
            DefaulGpfdistLocationFactory gpfdistLocationFactory = new DefaulGpfdistLocationFactory(new GpfdistServerConfig(server.getPort(),
                    server.getHost(),
                    gpfdistServerConfig.getMinThreads(),
                    gpfdistServerConfig.getMaxThreads(),
                    gpfdistServerConfig.getIdleTimeoutMs(),
                    gpfdistServerConfig.isSslEnabled()));
            DefaultExternalTableFormatConfigFactory externalTableFormatConfigFactory = new DefaultExternalTableFormatConfigFactory(dataFormatConfig);
            final DefaultGpfdistLoadMetadataFactory loadMetadataFactory =
                    new DefaultGpfdistLoadMetadataFactory(gpfdistLocationFactory, externalTableFormatConfigFactory);
            loadDataQueryExecutor = new LoadDataQueryExecutor(queryExecutorService,
                    dbcpService,
                    new CreateReadableExternalTableQueryFactory(),
                    new DefaultInsertDataQueryFactory(),
                    logger);
            unloadDataQueryExecutor = new UnloadDataQueryExecutor(queryExecutorService,
                    dbcpService,
                    new CreateWritableExternalTableQueryFactory(),
                    new UnloadInsertDataQueryFactory(readContextManager),
                    logger);
            gpfdistUnloadMetadataFactory = new DefaultGpfdistUnloadMetadataFactory(
                    gpfdistLocationFactory,
                    externalTableFormatConfigFactory);
            recordSinkProvider = new GpfdistRecordSinkProvider(recordProcessingExecutorService,
                    writeContextManager,
                    loadMetadataFactory,
                    writeBufferSize,
                    logger);
            StateManager stateManager = getStateManager();
            nodeIndexService = new ClusterStateNodeIndexService(nodeCount, host, stateManager, getLogger());
            registerNode(stateManager, host);
        } catch (Exception e) {
            String errMsg = "Failed to configure gpfdist service: " + e.getMessage();
            getLogger().error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    private void registerNode(final StateManager stateManager, final String hostname) throws Exception {
        for (int attempt = 0; attempt < MAX_REGISTER_ATTEMPTS; attempt++) {
            final StateMap state = stateManager.getState(Scope.CLUSTER);
            final Map<String, String> newState = new HashMap<>(state.toMap());
            final String currentHostsStr = newState.getOrDefault(KEY_HOSTS, "");
            final Set<String> hosts = new LinkedHashSet<>();
            if (!currentHostsStr.isEmpty()) {
                hosts.addAll(getNodesHostnames(currentHostsStr));
            }
            if (hosts.contains(hostname)) {
                getLogger().info("Node {} is already registered in cluster state", hostname);
                return;
            }
            hosts.add(hostname);
            final String updatedHostsStr = String.join(",", hosts);
            newState.put(KEY_HOSTS, updatedHostsStr);

            final boolean success = stateManager.replace(state, newState, Scope.CLUSTER);
            if (success) {
                getLogger().info("Registered node {}. Current hosts: {}", hostname, updatedHostsStr);
                return;
            } else {
                getLogger().debug("Failed to register node {} on attempt {} due to concurrent update, retrying",
                        hostname, attempt + 1);
            }
        }
        throw new IllegalStateException("Failed to register node " + hostname + " in cluster state after "
                + MAX_REGISTER_ATTEMPTS + " attempts");
    }


    @OnShutdown
    @OnDisabled
    public void cleanup() {
        if (server != null) {
            server.stop();
        }
    }

    private static String getLocalCanonicalHostname() {
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            return localHost.getCanonicalHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to get hostname");
        }
    }

    @Override
    public RecordSinkProvider getRecordSinkProvider() {
        return recordSinkProvider;
    }

    @Override
    public GreengageService getGreengageMetadataService() {
        return greengageService;
    }

    @Override
    public TransferDataQueryExecutor getLoadDataQueryExecutor() {
        return loadDataQueryExecutor;
    }

    @Override
    public TransferDataQueryExecutor getUnloadDataQueryExecutor() {
        return unloadDataQueryExecutor;
    }

    @Override
    public GpfdistUnloadMetadataFactory getGpfdistUnloadMetadataFactory() {
        return gpfdistUnloadMetadataFactory;
    }

    @Override
    public ContextManager<? extends Context> getReadContextManager() {
        return readContextManager;
    }

    @Override
    public NodeIndexService getNodeIndexService() {
        return nodeIndexService;
    }
}
