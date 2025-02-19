package org.apache.nifi.gpfdist.server;

import org.apache.nifi.gpfdist.server.config.GpfdistServerConfig;
import org.apache.nifi.gpfdist.server.servlet.GpfdistAsyncServlet;
import org.apache.nifi.gpfdist.server.servlet.GpfdistServletContextListener;
import org.apache.nifi.gpfdist.service.load.context.WriteContextManager;
import org.apache.nifi.gpfdist.service.load.process.RecordProcessorFactory;
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
    private static final String GPFDIST_CONTEXT_PATH = "/gpfdist";
    private final GpfdistServerConfig serverConfig;
    private volatile JettyServer server;
    private final WriteContextManager writeContextManager;
    private final RecordProcessorFactory recordProcessorFactory;
    private final ExecutorService recordProcessingExecutorService;
    private final ComponentLog logger;

    public DefaultGpfdistServer(final GpfdistServerConfig serverConfig,
                                final WriteContextManager writeContextManager,
                                final RecordProcessorFactory recordProcessorFactory,
                                final ExecutorService recordProcessingExecutorService,
                                ComponentLog logger) {
        this.serverConfig = serverConfig;
        this.writeContextManager = writeContextManager;
        this.recordProcessingExecutorService = recordProcessingExecutorService;
        this.logger = logger;
        this.recordProcessorFactory = recordProcessorFactory;
    }

    @Override
    public void start() {
        try {
            server = new JettyServer(serverConfig,
                    writeContextManager,
                    recordProcessorFactory,
                    recordProcessingExecutorService,
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

    public static class JettyServer {
        private final GpfdistServerConfig serverConfig;
        private final WriteContextManager writeContextManager;
        private final RecordProcessorFactory recordProcessorFactory;
        private final ExecutorService recordProcessingExecutorService;
        private final ComponentLog logger;
        private ServerConnector connector;
        private Server server;

        public JettyServer(final GpfdistServerConfig serverConfig,
                           final WriteContextManager writeContextManager,
                           final RecordProcessorFactory recordProcessorFactory,
                           final ExecutorService recordProcessingExecutorService,
                           ComponentLog logger) {
            this.writeContextManager = writeContextManager;
            this.recordProcessorFactory = recordProcessorFactory;
            this.recordProcessingExecutorService = recordProcessingExecutorService;
            this.logger = logger;
            this.serverConfig = serverConfig;
        }

        public void start() throws Exception {
            server = new Server(new QueuedThreadPool(serverConfig.getMaxThreads(),
                    serverConfig.getMinThreads(),
                    serverConfig.getIdleTimeoutMs()));
            connector = new ServerConnector(server);
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
                    recordProcessorFactory,
                    recordProcessingExecutorService,
                    logger));
            context.addServlet(GpfdistAsyncServlet.class, GPFDIST_READ_ENDPOINT);
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
    }
}
