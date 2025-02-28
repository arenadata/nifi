package org.apache.nifi.gpfdist.service.load.metadata.factory;

import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.ExternalTableFormat;
import org.apache.nifi.gpfdist.metadata.TableDescription;
import org.apache.nifi.gpfdist.service.load.metadata.GpfdistLoadMetadata;
import org.apache.nifi.gpfdist.service.metadata.ExternalTableFormatConfigFactory;
import org.apache.nifi.gpfdist.service.metadata.ExternalTableType;
import org.apache.nifi.gpfdist.service.metadata.GpfdistLocationFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.List;

import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.createExternalTableName;


public class DefaultGpfdistLoadMetadataFactory {
    private static final ExternalTableType EXTERNAL_TABLE_TYPE = ExternalTableType.READABLE;
    private final GpfdistLocationFactory gpfdistLocationFactory;
    private final ExternalTableFormatConfigFactory externalTableFormatConfigFactory;

    public DefaultGpfdistLoadMetadataFactory(final GpfdistLocationFactory gpfdistLocationFactory,
                                             final ExternalTableFormatConfigFactory externalTableFormatConfigFactory) {
        this.gpfdistLocationFactory = gpfdistLocationFactory;
        this.externalTableFormatConfigFactory = externalTableFormatConfigFactory;
    }

    public GpfdistLoadMetadata create(RecordSchema recordSchema, TableDescription tableMetadata, List<ColumnDescription> columnDescriptions) {
        ExternalTableFormat tableFormatConfig = externalTableFormatConfigFactory.create();
        String externalTable = createExternalTableName(EXTERNAL_TABLE_TYPE);
        String gpfdistLocation = gpfdistLocationFactory.create(externalTable, EXTERNAL_TABLE_TYPE);
        return new GpfdistLoadMetadata(externalTable,
                tableMetadata,
                columnDescriptions,
                tableFormatConfig,
                gpfdistLocation,
                recordSchema);
    }
}
