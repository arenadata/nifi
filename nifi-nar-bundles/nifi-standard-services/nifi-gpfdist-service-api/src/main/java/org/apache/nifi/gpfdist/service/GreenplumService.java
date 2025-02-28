package org.apache.nifi.gpfdist.service;

import org.apache.nifi.gpfdist.metadata.TableDescription;

import java.sql.DatabaseMetaData;

public interface GreenplumService {

    DatabaseMetaData getDatabaseMetadata();

    TableDescription getTableDescription(String schemaName, String tableName);
}
