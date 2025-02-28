package org.apache.nifi.gpfdist.service.load.metadata;

import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.ExternalTableFormat;
import org.apache.nifi.gpfdist.metadata.TableDescription;
import org.apache.nifi.gpfdist.service.metadata.BaseGpfdistMetadata;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.List;

public class GpfdistLoadMetadata extends BaseGpfdistMetadata {

    private final RecordSchema recordSchema;

    public GpfdistLoadMetadata(final String externalTable,
                               final TableDescription tableMetadata,
                               final List<ColumnDescription> columns,
                               final ExternalTableFormat externalTableFormatConfig,
                               final String gpfdistLocation,
                               final RecordSchema recordSchema) {
        super(tableMetadata, columns, externalTable, externalTableFormatConfig, gpfdistLocation);
        this.recordSchema = recordSchema;
    }

    public RecordSchema getRecordSchema() {
        return recordSchema;
    }
}
