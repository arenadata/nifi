package org.apache.nifi.gpfdist.service.greenplum.model;

import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.TableDescription;

import java.util.Map;

public class GreenplumTableDescription implements TableDescription {
    private final String schemaName;
    private final String tableName;
    private final Map<String, ColumnDescription> columns;

    public GreenplumTableDescription(final String schemaName,
                                     final String tableName,
                                     final Map<String, ColumnDescription> columns) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = columns;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public Map<String, ColumnDescription> getColumns() {
        return columns;
    }
}
