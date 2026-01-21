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

public class DropExternalTableQueryExecutor extends AbstractDataQueryExecutor {

    private final DropExternalTableQueryFactory dropExternalTableQueryFactory;

    public DropExternalTableQueryExecutor(final ExecutorService executorService,
                                          final DBCPService dbcpService,
                                          final DropExternalTableQueryFactory dropExternalTableQueryFactory,
                                          ComponentLog logger) {
        super(executorService, dbcpService, logger);
        this.dropExternalTableQueryFactory = dropExternalTableQueryFactory;
    }

    @Override
    protected GreengageStatement createStatement(GpfdistMetadata metadata, Connection connection) throws SQLException {
        return dropExternalTable(metadata, connection);
    }

    private GreengageStatement dropExternalTable(GpfdistMetadata metadata, Connection connection)
            throws SQLException {
        String sql = dropExternalTableQueryFactory.createQuery(metadata);
        Statement stmt = connection.createStatement();
        return new GreengageStatement() {
            @Override
            public void execute() throws SQLException {
                logger.debug("Executing drop external table query: {}", sql);
                stmt.execute(sql);
                logger.info("Executed drop external table query: {}", sql);
            }

            @Override
            public void cancel() throws SQLException {
                stmt.cancel();
                logger.info("Cancelled drop external table query: {}", sql);
            }
        };
    }
}
