package org.apache.nifi.gpfdist.metadata;

public interface ColumnDataType {
    String getName();

    GreenplumDataType getType();
}
