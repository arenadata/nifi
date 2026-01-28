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
        insertFromExternalTable(metadata, connection);
    }

    private void createReadableExternalTable(GpfdistMetadata metadata, Connection connection)
            throws SQLException {
        String sql = externalTableQueryFactory.createQuery(metadata);
        logger.info("Executing create readable external table query: {}", sql);
        connection.createStatement().execute(sql);
        logger.info("Executed create readable external table query: ", sql);
    }

    private void insertFromExternalTable(GpfdistMetadata metadata, Connection connection)
            throws SQLException {
        String sql = insertDataQueryFactory.create(metadata);
        logger.info("Executing insert query: {}", sql);
        connection.createStatement().execute(sql);
        logger.info("Executed insert into target table from external table query: {}", sql);
    }
}
