package org.apache.nifi.gpfdist.service.query;

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.service.TransferDataQueryExecutor;
import org.apache.nifi.logging.ComponentLog;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

public abstract class AbstractDataQueryExecutor implements TransferDataQueryExecutor {
    private final DBCPService dbcpService;
    private final ExecutorService executorService;
    protected final ComponentLog logger;

    public AbstractDataQueryExecutor(final ExecutorService executorService,
                                     final DBCPService dbcpService,
                                     ComponentLog logger) {
        this.dbcpService = dbcpService;
        this.executorService = executorService;
        this.logger = logger;
    }

    @Override
    public CompletableFuture<Void> execute(GpfdistMetadata metadata) {
        return CompletableFuture.runAsync(() -> {
            Connection connection = null;
            try {
                connection = dbcpService.getConnection();
                connection.setAutoCommit(false);
                connection.setReadOnly(false);
                executeQueries(metadata, connection);
                connection.commit();
                connection.close();
            } catch (Exception e) {
                String errMsg =
                        "Failed to execute data transfer query: " + String.join("; ", e.getMessage().split("\n"));
                logger.error(errMsg, e);
                throw new CompletionException(errMsg, e);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        logger.warn("Failed to close connection", e);
                    }
                }
            }
        }, executorService);
    }

    protected abstract void executeQueries(GpfdistMetadata metadata, Connection connection)
            throws SQLException;
}
