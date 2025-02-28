package org.apache.nifi.gpfdist.server.servlet;

import org.apache.nifi.gpfdist.service.load.context.WriteContextManager;
import org.apache.nifi.gpfdist.service.load.process.RecordProcessorFactory;
import org.apache.nifi.logging.ComponentLog;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.concurrent.ExecutorService;

import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.*;

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
