package org.apache.nifi.gpfdist.metadata;

public interface ColumnDescription {
    String getName();

    ColumnDataType getDataType();

    boolean isRequired();
}
