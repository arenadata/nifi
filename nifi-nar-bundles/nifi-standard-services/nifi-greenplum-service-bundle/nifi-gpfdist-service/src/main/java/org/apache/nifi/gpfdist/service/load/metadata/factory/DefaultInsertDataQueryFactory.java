package org.apache.nifi.gpfdist.service.load.metadata.factory;

import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.service.query.InsertDataQueryFactory;

import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.nifi.gpfdist.service.util.GreenplumUtil.getFullName;
import static org.apache.nifi.gpfdist.service.util.GreenplumUtil.quote;

public class DefaultInsertDataQueryFactory implements InsertDataQueryFactory {
    private static final String COLUMN_DELIMITER = ", ";

    @Override
    public String createInsertFromExternalTableQuery(final GpfdistMetadata loadMetadata) {
        String externalTableColumnNames = loadMetadata.getColumnDescriptions().stream()
                .map(colDesc -> quote(colDesc.getName()))
                .collect(Collectors.joining(COLUMN_DELIMITER));
        String targetTableColumnNames;
        targetTableColumnNames = externalTableColumnNames;
        return format("INSERT INTO %s (%s) SELECT %s FROM %s",
                getFullName(loadMetadata.getTableMetadata().getSchemaName(), loadMetadata.getTableMetadata().getTableName()),
                targetTableColumnNames,
                externalTableColumnNames,
                loadMetadata.getExternalTable());
    }
}
