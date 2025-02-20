package org.apache.nifi.gpfdist.service.load.serialization.csv;

import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.service.load.serialization.RecordSerializationService;
import org.apache.nifi.gpfdist.service.metadata.CsvFormatConfig;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class CsvRecordSerializationService implements RecordSerializationService {
    private final RecordWriter csvWriter;
    private final ByteArrayOutputStream outputStream;
    private final ComponentLog logger;
    private int serializedRecords;

    public CsvRecordSerializationService(final RecordSchema recordSchema,
                                         final List<ColumnDescription> columnDescriptions,
                                         final CsvFormatConfig csvFormatConfig,
                                         ComponentLog logger) {
        this.logger = logger;
        this.outputStream = new ByteArrayOutputStream();
        csvWriter = createCsvWriter(recordSchema, columnDescriptions, csvFormatConfig);
    }

    private CsvRecordSetWriter createCsvWriter(RecordSchema recordSchema,
                                               List<ColumnDescription> columnDescriptions,
                                               CsvFormatConfig csvFormatConfig) {
        try {
            return new CsvRecordSetWriter(csvFormatConfig.getCsvFormat(),
                    recordSchema,
                    outputStream,
                    false,
                    csvFormatConfig.getEncoding(),
                    columnDescriptions,
                    logger);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create csv record set writer", e);
        }
    }

    @Override
    public void append(Record record) {
        try {
            csvWriter.write(record);
            serializedRecords++;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write serialized record", e);
        }
    }

    @Override
    public int getSerializedRecordsCount() {
        return serializedRecords;
    }

    @Override
    public byte[] toByteArray() {
        try {
            csvWriter.flush();
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to get bytes from output stream", e);
        }
    }

    @Override
    public void reset() {
        outputStream.reset();
        serializedRecords = 0;
    }

    @Override
    public void close() {
        try {
            outputStream.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close output steam", e);
        }
    }
}
