package org.apache.nifi.gpfdist.service.load.process;

import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.ContextId;
import org.apache.nifi.gpfdist.metadata.TableDescription;
import org.apache.nifi.gpfdist.service.RecordSink;
import org.apache.nifi.gpfdist.service.RecordSinkProvider;
import org.apache.nifi.gpfdist.service.context.GpfdistContextId;
import org.apache.nifi.gpfdist.service.load.context.WriteContext;
import org.apache.nifi.gpfdist.service.load.context.WriteContextManager;
import org.apache.nifi.gpfdist.service.load.metadata.GpfdistLoadMetadata;
import org.apache.nifi.gpfdist.service.load.metadata.factory.DefaultGpfdistLoadMetadataFactory;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.List;
import java.util.concurrent.ExecutorService;

public class GpfdistRecordSinkProvider implements RecordSinkProvider {
    private final ExecutorService recordProcessingExecutorService;
    private final WriteContextManager contextManager;
    private final DefaultGpfdistLoadMetadataFactory loadMetadataFactory;
    private final int bufferSize;
    private final ComponentLog logger;

    public GpfdistRecordSinkProvider(final ExecutorService recordProcessingExecutorService,
                                     final WriteContextManager contextManager,
                                     final DefaultGpfdistLoadMetadataFactory loadMetadataFactory,
                                     final int bufferSize,
                                     final ComponentLog logger) {
        this.recordProcessingExecutorService = recordProcessingExecutorService;
        this.contextManager = contextManager;
        this.loadMetadataFactory = loadMetadataFactory;
        this.logger = logger;
        this.bufferSize = bufferSize;
    }

    @Override
    public RecordSink createRecordSink(final TableDescription tableMetadata,
                                       final List<ColumnDescription> columnDescriptions,
                                       final RecordSchema recordSchema) {
        GpfdistLoadMetadata metadata = loadMetadataFactory.create(recordSchema, tableMetadata, columnDescriptions);
        ContextId contextId = new GpfdistContextId(metadata.getExternalTable());
        WriteContext writeContext = new WriteContext(
                contextId,
                bufferSize,
                metadata,
                new GpfdistRecordProcessorProvider(),
                logger);
        contextManager.add(writeContext);
        return new GpfdistRecordSink(contextId, recordProcessingExecutorService, contextManager, logger);
    }
}
