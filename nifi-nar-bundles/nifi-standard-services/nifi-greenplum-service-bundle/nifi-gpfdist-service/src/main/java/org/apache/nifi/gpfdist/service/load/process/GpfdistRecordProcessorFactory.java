package org.apache.nifi.gpfdist.service.load.process;

import org.apache.nifi.gpfdist.metadata.DataFormat;
import org.apache.nifi.gpfdist.service.RecordProcessor;
import org.apache.nifi.gpfdist.server.request.GpfdistReadableRequest;
import org.apache.nifi.gpfdist.server.request.ReadableRequest;
import org.apache.nifi.gpfdist.service.load.context.WriteContext;
import org.apache.nifi.gpfdist.service.load.serialization.RecordSerializationService;
import org.apache.nifi.gpfdist.service.load.serialization.csv.CsvRecordSerializationService;
import org.apache.nifi.gpfdist.service.metadata.CsvFormatConfig;
import org.apache.nifi.gpfdist.service.metadata.DataFormatConfig;

import java.io.OutputStream;

public class GpfdistRecordProcessorFactory implements RecordProcessorFactory {
    private final DataFormatConfig dataFormatConfig;

    public GpfdistRecordProcessorFactory(final DataFormatConfig dataFormatConfig) {
        this.dataFormatConfig = dataFormatConfig;
    }

    @Override
    public RecordProcessor create(final ReadableRequest readableRequest,
                                  final WriteContext writeContext,
                                  final OutputStream outputStream) {
        return new GpfdistRecordProcessor((GpfdistReadableRequest) readableRequest,
                writeContext,
                outputStream,
                createRecordSerializationService(writeContext),
                writeContext.getLogger());
    }

    private RecordSerializationService createRecordSerializationService(WriteContext writeContext) {
        if (dataFormatConfig.getDataFormat() == DataFormat.CSV) {
            CsvFormatConfig csvFormatConfig = (CsvFormatConfig) dataFormatConfig;
            return new CsvRecordSerializationService(writeContext.getMetadata().getRecordSchema(),
                    writeContext.getMetadata().getColumnDescriptions(),
                    csvFormatConfig);
        } else {
            throw new UnsupportedOperationException("Unsupported DataFormat: " + dataFormatConfig);
        }
    }
}
