package org.apache.nifi.gpfdist.service.query;

import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;

public interface InsertDataQueryFactory {

    String createInsertFromExternalTableQuery(GpfdistMetadata loadMetadata);
}
