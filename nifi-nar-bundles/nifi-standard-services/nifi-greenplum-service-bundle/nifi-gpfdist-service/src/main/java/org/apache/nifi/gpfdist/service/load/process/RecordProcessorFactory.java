package org.apache.nifi.gpfdist.service.load.process;

import org.apache.nifi.gpfdist.server.request.ReadableRequest;
import org.apache.nifi.gpfdist.service.RecordProcessor;
import org.apache.nifi.gpfdist.service.load.context.WriteContext;

import java.io.OutputStream;

public interface RecordProcessorFactory {
    RecordProcessor create(ReadableRequest readableRequest, WriteContext writeContext, OutputStream outputStream);
}
