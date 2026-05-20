/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.gpfdist.service.load.process;

import org.apache.nifi.gpfdist.metadata.RecordProcessorId;
import org.apache.nifi.gpfdist.metadata.RecordProcessorLoadingResult;
import org.apache.nifi.gpfdist.service.RecordProcessor;
import org.apache.nifi.gpfdist.service.load.serialization.RecordSerializationService;
import org.apache.nifi.gpfdist.service.load.serialization.RecordsSerializationResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.Record;

import java.util.concurrent.TimeUnit;

public class GpfdistRecordProcessorNonBlocking implements RecordProcessor {
    private static final long FLUSH_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(500);

    private final RecordProcessorId id;
    private final GpfdistPacketBuilder packetBuilder;
    private final RecordSerializationService serializationService;
    private final GpfdistSegmentStream stream;
    private final RecordProcessorLoadingResult result;
    private final ComponentLog logger;
    private long lastFlushNanos = System.nanoTime();

    public GpfdistRecordProcessorNonBlocking(RecordProcessorId id,
                                             GpfdistPacketBuilder packetBuilder,
                                             RecordSerializationService serializationService,
                                             GpfdistSegmentStream stream,
                                             ComponentLog logger) {
        this.id = id;
        this.packetBuilder = packetBuilder;
        this.serializationService = serializationService;
        this.stream = stream;
        this.logger = logger;
        this.result = new RecordProcessorLoadingResult(id);
    }

    @Override
    public void process(Record record) {
        serializationService.append(record);
        result.incrementRecordCount();

        boolean batchReady = serializationService.getSerializedRecordsCount()
                >= GpfdistPacketBuilder.GPFDIST_PACKET_RECORD_BATCH_SIZE;
        boolean timeReady = serializationService.getSerializedRecordsCount() > 0
                && (System.nanoTime() - lastFlushNanos) >= FLUSH_INTERVAL_NANOS;

        if (batchReady || timeReady) {
            flushOrFail();
            lastFlushNanos = System.nanoTime();
        }
    }

    private void flushOrFail() {
        int count = serializationService.getSerializedRecordsCount();
        if (count <= 0) {
            return;
        }

        byte[] data = serializationService.toByteArray();
        byte[] pkt = packetBuilder.createDataPacket(new RecordsSerializationResult(data, count));

        if (!stream.offer(pkt)) {
            RuntimeException error = new RuntimeException("Backpressure: segment stream buffer overflow");
            byte[] errPkt = packetBuilder.createErrorPacket(error);
            stream.offer(errPkt);
            stream.complete();
            logger.error("Failed to process records", error);
            throw new RuntimeException("Backpressure: cannot enqueue gpfdist packet for " + id);
        }

        result.incrementRecordBytes(data.length);
        serializationService.reset();
    }

    @Override
    public void stop() {
        try {
            int remained = serializationService.getSerializedRecordsCount();
            if (remained > 0) {
                byte[] data = serializationService.toByteArray();
                byte[] pkt = packetBuilder.createDataPacket(new RecordsSerializationResult(data, remained));
                if (!stream.offer(pkt)) {
                    throw new RuntimeException("Backpressure while stopping: cannot enqueue gpfdist packet for  " + id);
                }
                result.incrementRecordBytes(data.length);
                serializationService.reset();
            }

            if (result.getRecordCount() > 0) {
                stream.offer(packetBuilder.createEndPacket());
            } else {
                stream.offer(packetBuilder.createSingleEmptyDataPacket());
            }
        } finally {
            completeStreamSafely();
        }
    }

    private void completeStreamSafely() {
        stream.complete();
        try {
            serializationService.close();
        } catch (Exception e) {
            logger.warn("Failed to close serialization service for processor {}: {}", id, e.getMessage(), e);
        }
    }

    @Override
    public void stopExceptionally(Throwable error) {
        try {
            stream.offer(packetBuilder.createErrorPacket(error));
        } finally {
            completeStreamSafely();
        }
    }

    @Override
    public RecordProcessorLoadingResult getResult() {
        return result;
    }

    @Override
    public RecordProcessorId getId() {
        return id;
    }

    @Override
    public String toString() {
        return "GpfdistRecordProcessorNonBlocking{id=" + id + "}";
    }
}
