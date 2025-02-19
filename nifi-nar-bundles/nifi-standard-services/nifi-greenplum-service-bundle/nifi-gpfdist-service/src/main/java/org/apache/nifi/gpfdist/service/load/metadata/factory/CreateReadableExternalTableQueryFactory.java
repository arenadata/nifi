package org.apache.nifi.gpfdist.service.load.metadata.factory;

import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.service.metadata.ExternalTableType;
import org.apache.nifi.gpfdist.service.query.AbstractExternalTableQueryFactory;

public class CreateReadableExternalTableQueryFactory extends AbstractExternalTableQueryFactory {
    @Override
    public String createQuery(final GpfdistMetadata metadata)
    {
        return createCommonQuery(metadata);
    }

    @Override
    public ExternalTableType getExternalTableType()
    {
        return ExternalTableType.READABLE;
    }
}
