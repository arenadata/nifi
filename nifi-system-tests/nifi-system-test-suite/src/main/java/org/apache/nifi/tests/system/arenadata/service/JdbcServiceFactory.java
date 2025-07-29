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
package org.apache.nifi.tests.system.arenadata.service;

import org.postgresql.ds.PGSimpleDataSource;

import static org.apache.nifi.tests.system.arenadata.util.ConfigUtil.getTestConfig;

public class JdbcServiceFactory {

    public JdbcService adbService() {
        PGSimpleDataSource adbDataSource = new PGSimpleDataSource();
        adbDataSource.setUrl(getTestConfig().getAdb().getUrl());
        adbDataSource.setUser(getTestConfig().getAdb().getUsername());
        if (getTestConfig().getAdb().getPassword() != null) {
            adbDataSource.setPassword(getTestConfig().getAdb().getPassword());
        }
        return new JdbcService(adbDataSource);
    }

    public JdbcService postgresService() {
        PGSimpleDataSource pgDataSource = new PGSimpleDataSource();
        pgDataSource.setUrl(getTestConfig().getPostgres().getUrl());
        pgDataSource.setUser(getTestConfig().getPostgres().getUsername());
        if (getTestConfig().getPostgres().getPassword() != null) {
            pgDataSource.setPassword(getTestConfig().getPostgres().getPassword());
        }
        return new JdbcService(pgDataSource);
    }
}
