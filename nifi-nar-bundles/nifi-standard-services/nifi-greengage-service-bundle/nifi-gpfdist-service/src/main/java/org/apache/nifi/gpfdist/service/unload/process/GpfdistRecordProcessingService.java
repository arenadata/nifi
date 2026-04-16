/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.nifi.gpfdist.service.unload.dto.GreengageChunkId;
import org.apache.nifi.gpfdist.service.unload.dto.UnloadingResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.Record;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class GpfdistRecordProcessingService
        implements RecordProcessingService {
    private static final int MINIMUM_RECORDS_BUFFER_SIZE = 1000;
    private final String processorTaskId;
    private final ArrayBlockingQueue<Record> queue;
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private final Map<GreengageChunkId, GpfdistChunkRequestProcessor> segmentDataProcessors = new ConcurrentHashMap<>();
    private final ComponentLog logger;

    public GpfdistRecordProcessingService(String processorTaskId, int maxRecordsBufferSize, ComponentLog logger) {
        this.processorTaskId = processorTaskId;
        this.queue = new ArrayBlockingQueue<>(Math.max(MINIMUM_RECORDS_BUFFER_SIZE, maxRecordsBufferSize));
        this.logger = logger;
    }

    @Override
    public void put(Record record) {
        try {
            queue.put(record);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while putting record to buffer", e);
        }
    }

    @Override
    public int drainTo(Collection<Record> collection, int maxElements, long waitMillis) {
        try {
            // First try to take one element, waiting up to waitMillis
            Record first = queue.poll(waitMillis, TimeUnit.MILLISECONDS);
            if (first == null) {
                return 0;
            }
            collection.add(first);
            // fast drain remaining up to maxElements-1
            if (maxElements - 1 > 0) {
                queue.drainTo(collection, maxElements - 1);
            }
            return collection.size();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return 0;
        }
    }

    @Override
    public int drainToImmediate(Collection<Record> collection, int maxElements) {
        return queue.drainTo(collection, maxElements);
    }

    @Override
    public void stop() {
        segmentDataProcessors.values().forEach(GpfdistChunkRequestProcessor::stop);
        logger.info("Stopped gpfdist record processing service for taskId {}", processorTaskId);
    }

    @Override
    public boolean isFinished() {
        return queue.isEmpty() && isStopped();
    }

    private boolean isStopped() {
        return segmentDataProcessors.values().stream()
                .allMatch(GpfdistChunkRequestProcessor::getStopped);
    }

    @Override
    public void clear() {
        queue.clear();
        logger.info("Cleared buffer records queue for taskId {}", processorTaskId);
    }

    @Override
    public void resetForNextCycle() {
        clear();
        segmentDataProcessors.clear();
        isStopped.set(false);
        logger.info("Reset gpfdist record processing service for next cycle, taskId {}", processorTaskId);
    }

    @Override
    public void addChunkRequestProcessor(GpfdistChunkRequestProcessor requestProcessor) {
        segmentDataProcessors.putIfAbsent(requestProcessor.getChunkId(), requestProcessor);
    }

    @Override
    public GpfdistChunkRequestProcessor getChunkRequestProcessor(GreengageChunkId chunkId) {
        return Optional.ofNullable(segmentDataProcessors.get(chunkId))
                .orElseThrow(() -> new IllegalStateException(
                        "Segment request processor for greengageChunkId: " + chunkId + " does not exist"));
    }

    @Override
    public Collection<UnloadingResult> getResult() {
        return segmentDataProcessors.values().stream()
                .map(GpfdistChunkRequestProcessor::getResult)
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        GpfdistRecordProcessingService that = (GpfdistRecordProcessingService) o;
        return Objects.equals(processorTaskId, that.processorTaskId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(processorTaskId);
    }

    @Override
    public String toString() {
        return "GpfdistRecordProcessingService{" +
                "processorTaskId='" + processorTaskId + '\'' +
                ", isStopped=" + isStopped +
                ", segmentDataProcessors=" + segmentDataProcessors +
                '}';
    }
}
