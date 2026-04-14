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
import org.apache.nifi.gpfdist.service.CancellableQuery;
import org.apache.nifi.gpfdist.service.TransferDataQueryExecutor;
import org.apache.nifi.logging.ComponentLog;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

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
        return executeCancellable(metadata).future();
    }

    @Override
    public CancellableQuery executeCancellable(GpfdistMetadata metadata) {
        final AtomicReference<Connection> connRef = new AtomicReference<>();
        final AtomicReference<GreengageStatement> stmtRef = new AtomicReference<>();
        final CompletableFuture<Void> future = new CompletableFuture<>();
        executorService.submit(() -> {
            Connection connection = null;
            try {
                connection = dbcpService.getConnection();
                connRef.set(connection);
                connection.setAutoCommit(false);
                connection.setReadOnly(false);
                GreengageStatement ggStmt = createStatement(metadata, connection);
                stmtRef.set(ggStmt);
                ggStmt.execute();
                connection.commit();
                future.complete(null);
            } catch (Throwable e) {
                final String errMsg = "Failed to execute data transfer query: " +
                        String.join("; ", String.valueOf(e.getMessage()).split("\n"));
                logger.error(errMsg, e);
                tryRollback(connection);
                future.completeExceptionally(new RuntimeException(errMsg, e));
            } finally {
                closeQuietly(connection);
                connRef.set(null);
                stmtRef.set(null);
            }
        });

        return new CancellableQuery() {
            @Override
            public CompletableFuture<Void> future() {
                return future;
            }

            @Override
            public void cancel() {
                GreengageStatement stmt = stmtRef.get();
                if (stmt != null) {
                    try {
                        stmt.cancel();
                    } catch (Exception e) {
                        logger.warn("Failed to cancel statement", e);
                    }
                }
                Connection conn = connRef.get();
                if (conn != null) {
                    try {
                        conn.abort(Runnable::run);
                    } catch (Exception abortEx) {
                        closeQuietly(conn);
                    }
                }
                future.cancel(true);
            }
        };
    }

    protected abstract GreengageStatement createStatement(GpfdistMetadata metadata, Connection connection)
            throws SQLException;

    private void tryRollback(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.rollback();
        } catch (Exception ignore) {
        }
    }

    private void closeQuietly(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (Exception e) {
            logger.warn("Failed to close connection", e);
        }
    }
}
