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
package org.apache.nifi.gpfdist.service.query;

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.logging.ComponentLog;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;

public class CreateExternalTableQueryExecutor extends AbstractDataQueryExecutor {

    private final CreateExternalTableQueryFactory externalTableQueryFactory;

    public CreateExternalTableQueryExecutor(final ExecutorService executorService,
                                            final DBCPService dbcpService,
                                            final CreateExternalTableQueryFactory externalTableQueryFactory,
                                            ComponentLog logger) {
        super(executorService, dbcpService, logger);
        this.externalTableQueryFactory = externalTableQueryFactory;
    }

    @Override
    protected GreengageStatement createStatement(GpfdistMetadata metadata, Connection connection) throws SQLException {
        return createExternalTable(metadata, connection);
    }

    private GreengageStatement createExternalTable(GpfdistMetadata metadata, Connection connection)
            throws SQLException {
        String sql = externalTableQueryFactory.createQuery(metadata);
        Statement stmt = connection.createStatement();
        return new GreengageStatement() {
            @Override
            public void execute() throws SQLException {
                logger.info("Executing create {} external table query: {}", externalTableQueryFactory.getExternalTableType(), sql);
                stmt.execute(sql);
                logger.info("Executed create {} external table query: {}", externalTableQueryFactory.getExternalTableType(), sql);
            }

            @Override
            public void cancel() {
                try {
                    stmt.cancel();
                    logger.info("Cancel {} external table query: {}", externalTableQueryFactory.getExternalTableType(), sql);
                } catch (Exception e) {
                    logger.warn("Failed to cancel statement", e);
                }
            }
        };
    }
}
