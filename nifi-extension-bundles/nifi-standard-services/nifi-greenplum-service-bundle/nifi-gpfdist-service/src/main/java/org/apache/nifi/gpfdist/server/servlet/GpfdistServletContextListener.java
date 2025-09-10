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

import org.apache.nifi.gpfdist.service.load.context.WriteContextManager;
import org.apache.nifi.gpfdist.service.load.process.RecordProcessorFactory;
import org.apache.nifi.logging.ComponentLog;

import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import java.util.concurrent.ExecutorService;

import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.COMPONENT_LOG_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.RECORD_PROCESSING_EXECUTOR_SERVICE_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.RECORD_PROCESSOR_FACTORY_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.WRITE_CONTEXT_MANAGER_ATTR;

public class GpfdistServletContextListener implements ServletContextListener {

    private final WriteContextManager writeContextManager;
    private final RecordProcessorFactory recordProcessorFactory;
    private final ExecutorService executorService;
    private final ComponentLog logger;

    public GpfdistServletContextListener(final WriteContextManager writeContextManager,
                                         final RecordProcessorFactory recordProcessorFactory,
                                         final ExecutorService executorService,
                                         ComponentLog logger) {
        this.writeContextManager = writeContextManager;
        this.recordProcessorFactory = recordProcessorFactory;
        this.executorService = executorService;
        this.logger = logger;
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        ServletContext servletContext = sce.getServletContext();
        servletContext.setAttribute(WRITE_CONTEXT_MANAGER_ATTR, writeContextManager);
        servletContext.setAttribute(RECORD_PROCESSOR_FACTORY_ATTR, recordProcessorFactory);
        servletContext.setAttribute(RECORD_PROCESSING_EXECUTOR_SERVICE_ATTR, executorService);
        servletContext.setAttribute(COMPONENT_LOG_ATTR, logger);
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        //stopped
    }
}
