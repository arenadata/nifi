package org.apache.nifi.gpfdist.service.query;

import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;

import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.apache.nifi.gpfdist.service.util.GreenplumUtil.quote;

public abstract class AbstractExternalTableQueryFactory implements CreateExternalTableQueryFactory {

    protected String createCommonQuery(final GpfdistMetadata metadata) {
        return format(
                "CREATE %s EXTERNAL TEMPORARY TABLE %s (%s) LOCATION ('%s') FORMAT '%s' (DELIMITER '%s' NULL AS '%s') ENCODING '%s'",
                getExternalTableType().name(),
                metadata.getExternalTable(),
                getColumnDefinition(metadata),
                metadata.getGpfdistLocation(),
                metadata.getExternalTableFormatConfig().getDataFormat().name(),
                metadata.getExternalTableFormatConfig().getDelimiter(),
                metadata.getExternalTableFormatConfig().getNullValue(),
                metadata.getExternalTableFormatConfig().getEncoding());
    }

    protected String getColumnDefinition(GpfdistMetadata metadata) {
        return IntStream.range(0, metadata.getColumnDescriptions().size())
                .boxed()
                .map(i -> {
                    ColumnDescription columnDescription = metadata.getColumnDescriptions().get(i);
                    return quote(columnDescription.getName()) + " " + columnDescription.getDataType().getName();
                })
                .collect(joining(","));
    }
}
