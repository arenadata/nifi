package org.apache.nifi.gpfdist.service;

import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.TableDescription;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.List;

public interface RecordSinkProvider {
    RecordSink createRecordSink(TableDescription tableDescription,
                                List<ColumnDescription> insertColumnDescriptions,
                                RecordSchema recordSchema);
}
