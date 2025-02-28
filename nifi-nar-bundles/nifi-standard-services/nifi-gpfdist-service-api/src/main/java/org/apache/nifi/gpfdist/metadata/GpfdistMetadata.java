package org.apache.nifi.gpfdist.metadata;

import java.util.List;

public interface GpfdistMetadata {
    TableDescription getTableMetadata();

    String getExternalTable();

    ExternalTableFormat getExternalTableFormatConfig();

    String getGpfdistLocation();

    List<ColumnDescription> getColumnDescriptions();
}
