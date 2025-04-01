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
