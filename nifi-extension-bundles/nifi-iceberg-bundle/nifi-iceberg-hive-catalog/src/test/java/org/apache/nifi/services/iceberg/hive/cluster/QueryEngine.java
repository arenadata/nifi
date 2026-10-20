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
package org.apache.nifi.services.iceberg.hive.cluster;

import java.util.List;

/**
 * SQL engine running in the cluster that reads and writes Iceberg tables through the shared Hive Metastore
 */
public interface QueryEngine {

    /**
     * Run SQL statements and return the result rows
     *
     * @param sql SQL statements separated with semicolons
     * @return Result rows with tab separated column values and without a header line
     * @throws Exception Thrown when the engine failed to run the statements
     */
    List<String> query(String sql) throws Exception;

    /**
     * Run SQL statements expected to fail and return the output of the engine
     *
     * @param sql SQL statements separated with semicolons
     * @return Result rows of statements completed before the failure and the complete error output
     * @throws Exception Thrown when the statements succeeded, timed out or the command failed
     */
    FailedQuery queryFailure(String sql) throws Exception;

    /**
     * Output of SQL statements that failed
     *
     * @param rows Result rows printed before the failure
     * @param output Complete error output followed by standard output
     */
    record FailedQuery(List<String> rows, String output) {
    }
}
