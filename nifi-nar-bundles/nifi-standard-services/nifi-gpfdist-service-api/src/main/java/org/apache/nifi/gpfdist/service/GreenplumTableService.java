package org.apache.nifi.gpfdist.service;

import org.apache.nifi.gpfdist.metadata.TableDescription;

public interface GreenplumTableService {

    TableDescription getTableDescription(String schemaName, String tableName);
}
