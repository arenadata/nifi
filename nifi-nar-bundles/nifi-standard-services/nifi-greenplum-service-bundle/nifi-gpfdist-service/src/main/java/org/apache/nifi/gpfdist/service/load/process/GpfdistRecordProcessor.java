package org.apache.nifi.gpfdist.service.load.process;

import org.apache.nifi.gpfdist.server.request.GpfdistReadableRequest;
import org.apache.nifi.gpfdist.service.RecordProcessor;
import org.apache.nifi.gpfdist.service.load.context.WriteContext;
import org.apache.nifi.gpfdist.service.load.metadata.LoadingSegmentResult;
import org.apache.nifi.gpfdist.service.load.serialization.RecordSerializationService;
import org.apache.nifi.gpfdist.service.load.serialization.RecordsSerializationResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.io.OutputStream;

import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.createGpfdistFileName;

public class GpfdistRecordProcessor implements RecordProcessor {
    private static final int BATCH_SIZE = 1000;
    private final GpfdistReadableRequest request;
    private final WriteContext writeContext;
    private final OutputStream outputStream;
    private final GpfdistPacketBuilder packetBuilder;
    private final RecordSerializationService recordSerializationService;
    private final ComponentLog logger;
    private long bytesWritten;
    private long processedBytes;
    private long processedRows;

    public GpfdistRecordProcessor(final GpfdistReadableRequest request,
                                  final WriteContext writeContext,
                                  final OutputStream outputStream,
                                  final RecordSerializationService recordSerializationService,
                                  ComponentLog logger) {
        this.writeContext = writeContext;
        this.outputStream = outputStream;
        this.logger = logger;
        this.request = request;
        this.recordSerializationService = recordSerializationService;
        packetBuilder = new GpfdistPacketBuilder(createGpfdistFileName(writeContext.getMetadata().getExternalTable()));
    }

    @Override
    public void process(Record record) {
        try {
            recordSerializationService.append(record);
            if (recordSerializationService.getSerializedRecordsCount() >= BATCH_SIZE) {
                byte[] data = recordSerializationService.toByteArray();
                outputStream.write(packetBuilder.createDataPacket(new RecordsSerializationResult(data,
                        recordSerializationService.getSerializedRecordsCount())));
                bytesWritten += data.length;
                processedBytes += bytesWritten;
                recordSerializationService.reset();
            }
            if (bytesWritten >= writeContext.getBufferSize()) {
                outputStream.flush();
                bytesWritten = 0;
            }
            processedRows++;
        } catch (Exception e) {
            throw new RuntimeException("Failed to process record: " + e.getMessage(), e);
        }
    }

    @Override
    public void stop() {
        try {
            Throwable error = writeContext.getResult().getError().get();
            if (error != null) {
                writePacket(packetBuilder.createErrorPacket(error));
                logger.warn("Stopped writing process with error. Sent error packet data for request: {}", error);
            } else {
                if (processedRows > 0) {
                    int remainedRecordsCount = recordSerializationService.getSerializedRecordsCount();
                    if (remainedRecordsCount > 0) {
                        byte[] data = recordSerializationService.toByteArray();
                        processedBytes += data.length;
                        writePacket(packetBuilder.createDataPacket(new RecordsSerializationResult(data, remainedRecordsCount)));
                    }
                    writePacket(packetBuilder.createEndPacket());
                    logger.info("Stopped writing process. Sent finished packet data for request: {}", request);
                } else {
                    writePacket(packetBuilder.createSingleEmptyDataPacket());
                    logger.info("Stopped writing process. Sent finished single empty packet data for request: {}", request);
                }
            }
            LoadingSegmentResult segmentResult = new LoadingSegmentResult(request.getSegmentId(), processedRows, processedBytes);
            writeContext.getResult().getSegmentsCount().compareAndSet(0, request.getSegmentsCount());
            writeContext.getResult().getSegmentResults().put(segmentResult.getSegmentId(), segmentResult);
            logger.info("Processing records segment result: {}", segmentResult);
            close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void writePacket(byte[] dataPacket) {
        try {
            outputStream.write(dataPacket);
            outputStream.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write end packet data: " + e.getMessage(), e);
        }
    }

    private void close() {
        try {
            outputStream.close();
            recordSerializationService.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close processing data stream: " + e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return "GpfdistRecordProcessor{" +
                "request=" + request +
                ", processedBytes=" + processedBytes +
                ", processedRows=" + processedRows +
                '}';
    }
}
