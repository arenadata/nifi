package org.apache.nifi.gpfdist.service.greenplum.model;

import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.metadata.ColumnDescription;

public class GreenplumColumnDescription implements ColumnDescription {
    private final String columnName;
    private final ColumnDataType dataType;
    private final boolean required;

    public GreenplumColumnDescription(final String columnName,
                                      final ColumnDataType dataType,
                                      final boolean required) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.required = required;
    }

    @Override
    public String getName() {
        return columnName;
    }

    @Override
    public ColumnDataType getDataType() {
        return dataType;
    }

    @Override
    public boolean isRequired() {
        return required;
    }
}
