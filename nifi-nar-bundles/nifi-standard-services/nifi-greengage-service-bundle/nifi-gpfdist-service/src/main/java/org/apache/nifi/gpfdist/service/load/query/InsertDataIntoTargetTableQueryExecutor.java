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
import org.apache.nifi.gpfdist.service.query.InsertDataQueryFactory;
import org.apache.nifi.logging.ComponentLog;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class InsertDataIntoTargetTableQueryExecutor extends AbstractDataQueryExecutor {
    private final InsertDataQueryFactory insertDataQueryFactory;

    public InsertDataIntoTargetTableQueryExecutor(final ExecutorService executorService,
                                                  final DBCPService dbcpService,
                                                  final InsertDataQueryFactory insertDataQueryFactory,
                                                  ComponentLog logger) {
        super(executorService, dbcpService, logger);
        this.insertDataQueryFactory = insertDataQueryFactory;
    }

    @Override
    protected void executeQueries(GpfdistMetadata metadata, Connection connection, AtomicReference<Statement> stmtRef) throws SQLException {
        insertFromExternalTable(metadata, connection, stmtRef);
    }

    private void insertFromExternalTable(GpfdistMetadata metadata, Connection connection, AtomicReference<Statement> stmtRef)
            throws SQLException {
        String sql = insertDataQueryFactory.create(metadata);
        logger.debug("Executing insert query: {}", sql);
        Statement stmt = connection.createStatement();
        stmtRef.set(stmt);
        stmt.execute(sql);
        logger.info("Executed insert into target table from external table query: {}", sql);
    }
}
