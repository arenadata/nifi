package org.apache.nifi.gpfdist.metadata;

import java.util.Map;

public interface TableDescription {
    String getSchemaName();

    String getTableName();

    Map<String, ColumnDescription> getColumns();
}
