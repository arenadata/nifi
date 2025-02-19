package org.apache.nifi.gpfdist.metadata;

import java.util.Map;

public interface TableDescription {
    String getTableName();

    String getSchemaName();

    String getCatalogName();

    Map<String, ColumnDescription> getColumns();
}
