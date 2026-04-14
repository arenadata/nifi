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
package org.apache.nifi.gpfdist.service.unload.process;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class FlowFileGenerator {
    private static final String GREENGAGE_UNLOADING_ATTR = "greengage.unloading";
    private final RecordSetWriterFactory writerFactory;
    private final RecordSchema recordSchema;
    private final int batchRecordCount;
    private final long pullTimeoutMillis;
    private final int maxFlowFilesPerTrigger;
    private final ComponentLog logger;

    public FlowFileGenerator(RecordSetWriterFactory writerFactory,
                             RecordSchema recordSchema,
                             int batchRecordCount,
                             long pullTimeoutMillis,
                             int maxFlowFilesPerTrigger,
                             ComponentLog logger) {
        this.writerFactory = writerFactory;
        this.recordSchema = recordSchema;
        this.batchRecordCount = Math.max(1000, batchRecordCount);
        this.pullTimeoutMillis = pullTimeoutMillis;
        this.maxFlowFilesPerTrigger = maxFlowFilesPerTrigger;
        this.logger = logger;
    }

    public List<FlowFile> createFlowFiles(ProcessSession session,
                                          String processorTaskId,
                                          RecordProcessingService recordProcessingService) {
        List<FlowFile> created = new ArrayList<>();
        int flowFilesCreated = 0;
        while (flowFilesCreated < maxFlowFilesPerTrigger) {
            List<Record> buffer = new ArrayList<>(batchRecordCount);
            int drained = recordProcessingService.drainTo(buffer, batchRecordCount, pullTimeoutMillis);
            if (drained == 0) {
                logger.trace("No records drained for task {} within {} ms", processorTaskId, pullTimeoutMillis);
                break;
            }
            logger.trace("Initially fill buffer by {} records for task {}", drained, processorTaskId);

            int remaining = batchRecordCount - buffer.size();
            if (remaining > 0) {
                int more = recordProcessingService.drainToImmediate(buffer, remaining);
                logger.trace("Additionally fill buffer by {} records for task {}", more, processorTaskId);
            }

            FlowFile ff = writeBufferToFlowFile(session, buffer, processorTaskId);
            if (ff != null) {
                created.add(ff);
                flowFilesCreated++;
            } else {
                break;
            }
        }
        return created;
    }

    private FlowFile writeBufferToFlowFile(ProcessSession session, List<Record> buffer, String taskId) {
        if (buffer.isEmpty()) {
            return null;
        }
        FlowFile ff = session.create();
        final long records = buffer.size();
        final long createdAt = System.currentTimeMillis();
        final String uuid = UUID.randomUUID().toString();

        ff = session.putAttribute(ff, GREENGAGE_UNLOADING_ATTR + ".taskId", taskId);
        ff = session.putAttribute(ff, GREENGAGE_UNLOADING_ATTR + ".records", String.valueOf(records));
        ff = session.putAttribute(ff, GREENGAGE_UNLOADING_ATTR + ".created", String.valueOf(createdAt));
        ff = session.putAttribute(ff, GREENGAGE_UNLOADING_ATTR + ".batchId", uuid);

        final List<Record> toWrite = new ArrayList<>(buffer);
        try {
            session.write(ff, out -> {
                try (RecordSetWriter writer = writerFactory.createWriter(logger, recordSchema, out, Collections.emptyMap())) {
                    for (Record rec : toWrite) {
                        writer.write(rec);
                    }
                    writer.flush();
                } catch (SchemaNotFoundException e) {
                    throw new RuntimeException(e);
                }
            });
            logger.debug("Successfully wrote FlowFile with {} records for task {}", records, taskId);
            return ff;
        } catch (Exception e) {
            logger.error("Failed to write FlowFile for task {}: {}", taskId, e.getMessage(), e);
            session.remove(ff);
            return null;
        }
    }

    @Override
    public String toString() {
        return "FlowFileProducer{" +
                "recordSchema=" + recordSchema +
                ", batchRecordCount=" + batchRecordCount +
                ", pullTimeoutMillis=" + pullTimeoutMillis +
                '}';
    }
}
