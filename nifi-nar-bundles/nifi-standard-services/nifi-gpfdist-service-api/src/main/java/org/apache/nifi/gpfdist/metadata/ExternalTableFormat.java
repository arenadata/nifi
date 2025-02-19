package org.apache.nifi.gpfdist.metadata;

public interface ExternalTableFormat {
    String getDelimiter();

    String getEncoding();

    String getNullValue();

    DataFormat getDataFormat();
}
