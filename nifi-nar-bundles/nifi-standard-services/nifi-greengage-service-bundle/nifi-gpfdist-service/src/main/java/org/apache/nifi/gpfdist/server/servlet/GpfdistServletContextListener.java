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
package org.apache.nifi.gpfdist.server.servlet;

import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextManager;
import org.apache.nifi.gpfdist.metadata.GpfidstLoadConfig;
import org.apache.nifi.gpfdist.service.load.process.RecordProcessorFactory;
import org.apache.nifi.gpfdist.service.unload.process.InputDataProcessorFactory;
import org.apache.nifi.logging.ComponentLog;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.concurrent.ExecutorService;

import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.COMPONENT_LOG_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.INPUT_DATA_PROCESSOR_FACTORY_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.LOAD_CONFIG;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.READ_CONTEXT_MANAGER_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.RECORD_PROCESSING_EXECUTOR_SERVICE_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.RECORD_PROCESSOR_FACTORY_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.WRITE_CONTEXT_MANAGER_ATTR;

public class GpfdistServletContextListener implements ServletContextListener {

    private final ContextManager<Context> writeContextManager;
    private final ContextManager<Context> readContextManager;
    private final RecordProcessorFactory recordProcessorFactory;
    private final InputDataProcessorFactory inputDataProcessorFactory;
    private final ExecutorService gpfdistRequestExecutorService;
    private final GpfidstLoadConfig gpfidstLoadConfig;
    private final ComponentLog logger;

    public GpfdistServletContextListener(final ContextManager<Context> writeContextManager,
                                         final ContextManager<Context> readContextManager,
                                         final RecordProcessorFactory recordProcessorFactory,
                                         final InputDataProcessorFactory inputDataProcessorFactory,
                                         final ExecutorService gpfdistRequestExecutorService,
                                         final GpfidstLoadConfig gpfidstLoadConfig,
                                         ComponentLog logger) {
        this.writeContextManager = writeContextManager;
        this.readContextManager = readContextManager;
        this.recordProcessorFactory = recordProcessorFactory;
        this.inputDataProcessorFactory = inputDataProcessorFactory;
        this.gpfdistRequestExecutorService = gpfdistRequestExecutorService;
        this.gpfidstLoadConfig = gpfidstLoadConfig;
        this.logger = logger;
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        ServletContext servletContext = sce.getServletContext();
        servletContext.setAttribute(WRITE_CONTEXT_MANAGER_ATTR, writeContextManager);
        servletContext.setAttribute(READ_CONTEXT_MANAGER_ATTR, readContextManager);
        servletContext.setAttribute(INPUT_DATA_PROCESSOR_FACTORY_ATTR, inputDataProcessorFactory);
        servletContext.setAttribute(RECORD_PROCESSOR_FACTORY_ATTR, recordProcessorFactory);
        servletContext.setAttribute(RECORD_PROCESSING_EXECUTOR_SERVICE_ATTR, gpfdistRequestExecutorService);
        servletContext.setAttribute(LOAD_CONFIG, gpfidstLoadConfig);
        servletContext.setAttribute(COMPONENT_LOG_ATTR, logger);
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        //stopped
    }
}
