package org.apache.nifi.gpfdist.service.query;

import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.service.metadata.ExternalTableType;

public interface CreateExternalTableQueryFactory {
    String createQuery(GpfdistMetadata metadata);

    ExternalTableType getExternalTableType();
}
