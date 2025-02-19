package org.apache.nifi.gpfdist.service.metadata;

import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.ExternalTableFormat;
import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.metadata.TableDescription;

import java.util.List;

public class BaseGpfdistMetadata implements GpfdistMetadata {
    private final TableDescription tableMetadata;
    private final List<ColumnDescription> columnDescriptions;
    private final String externalTable;
    private final ExternalTableFormat externalTableFormatConfig;
    private final String gpfdistLocation;

    public BaseGpfdistMetadata(final TableDescription tableMetadata,
                               final List<ColumnDescription> columns,
                               final String externalTable,
                               final ExternalTableFormat externalTableFormatConfig,
                               final String gpfdistLocation) {
        this.tableMetadata = tableMetadata;
        this.columnDescriptions = columns;
        this.externalTable = externalTable;
        this.externalTableFormatConfig = externalTableFormatConfig;
        this.gpfdistLocation = gpfdistLocation;
    }

    @Override
    public TableDescription getTableMetadata() {
        return tableMetadata;
    }

    @Override
    public String getExternalTable() {
        return externalTable;
    }

    @Override
    public ExternalTableFormat getExternalTableFormatConfig() {
        return externalTableFormatConfig;
    }

    @Override
    public String getGpfdistLocation() {
        return gpfdistLocation;
    }

    @Override
    public List<ColumnDescription> getColumnDescriptions() {
        return columnDescriptions;
    }
}
