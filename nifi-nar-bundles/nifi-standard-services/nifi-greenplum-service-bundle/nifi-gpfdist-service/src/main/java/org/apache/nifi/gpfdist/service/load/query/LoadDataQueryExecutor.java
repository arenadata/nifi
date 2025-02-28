package org.apache.nifi.gpfdist.service.load.query;

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.service.query.AbstractDataQueryExecutor;
import org.apache.nifi.gpfdist.service.query.CreateExternalTableQueryFactory;
import org.apache.nifi.gpfdist.service.query.InsertDataQueryFactory;
import org.apache.nifi.logging.ComponentLog;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

public class LoadDataQueryExecutor extends AbstractDataQueryExecutor {

    private final CreateExternalTableQueryFactory externalTableQueryFactory;
    private final InsertDataQueryFactory insertDataQueryFactory;

    public LoadDataQueryExecutor(final ExecutorService executorService,
                                 final DBCPService dbcpService,
                                 final CreateExternalTableQueryFactory externalTableQueryFactory,
                                 final InsertDataQueryFactory insertDataQueryFactory,
                                 ComponentLog logger) {
        super(executorService, dbcpService, logger);
        this.externalTableQueryFactory = externalTableQueryFactory;
        this.insertDataQueryFactory = insertDataQueryFactory;
    }

    @Override
    protected void executeQueries(GpfdistMetadata metadata, Connection connection) throws SQLException {
        createReadableExternalTable(metadata, connection);
        insertIntoExternalTable(metadata, connection);
    }

    private void createReadableExternalTable(GpfdistMetadata metadata, Connection connection)
            throws SQLException {
        String sql = externalTableQueryFactory.createQuery(metadata);
        logger.info("Executing create readable external table query: {}", sql);
        connection.createStatement().execute(sql);
        logger.info("Executed create readable external table query: ", sql);
    }

    private void insertIntoExternalTable(GpfdistMetadata metadata, Connection connection)
            throws SQLException {
        String sql = insertDataQueryFactory.createInsertFromExternalTableQuery(metadata);
        logger.info("Executing insert query: {}", sql);
        connection.createStatement().execute(sql);
        logger.info("Executed insert into target table from external table query: {}", sql);
    }
}
