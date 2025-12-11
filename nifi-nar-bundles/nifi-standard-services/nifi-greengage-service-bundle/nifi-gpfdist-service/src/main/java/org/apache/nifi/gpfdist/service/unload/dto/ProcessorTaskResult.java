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
package org.apache.nifi.gpfdist.service.unload.dto;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ProcessorTaskResult {
    private final String processorTaskId;
    private final List<SegmentResult> segments;
    private final long totalRecordsCount;

    public ProcessorTaskResult(String processorTaskId,
                               List<SegmentResult> segments,
                               long totalRecordsCount) {
        this.processorTaskId = processorTaskId;
        this.segments = segments;
        this.totalRecordsCount = totalRecordsCount;
    }

    public static ProcessorTaskResult aggregate(String processorTaskId,
                                                Collection<UnloadingResult> unloadingResults) {
        Map<Integer, Map<String, Long>> segmentTxMap = new LinkedHashMap<>();
        long totalPerTask = 0L;
        for (UnloadingResult result : unloadingResults) {
            if (result == null) {
                continue;
            }
            long recordsCount = result.getRecordCount();
            totalPerTask += recordsCount;
            ProcessingChunkId chunkId = result.getChunkId();
            if (chunkId == null) {
                continue;
            }
            int segmentId = chunkId.getSegmentId();
            String txId = chunkId.getTxId();
            Map<String, Long> txMap = segmentTxMap.computeIfAbsent(
                    segmentId, id -> new LinkedHashMap<>());
            txMap.merge(txId, recordsCount, Long::sum);
        }

        List<SegmentResult> segments = segmentTxMap.entrySet().stream()
                .map(entry -> {
                    int segmentId = entry.getKey();
                    Map<String, Long> txMap = entry.getValue();

                    List<TransactionResult> transactions = txMap.entrySet().stream()
                            .map(txEntry -> new TransactionResult(
                                    txEntry.getKey(),
                                    txEntry.getValue()
                            ))
                            .collect(Collectors.toList());

                    long totalPerSegment = txMap.values().stream()
                            .mapToLong(Long::longValue)
                            .sum();

                    return new SegmentResult(segmentId, transactions, totalPerSegment);
                })
                .collect(Collectors.toList());

        return new ProcessorTaskResult(processorTaskId, segments, totalPerTask);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ProcessorTaskResult that = (ProcessorTaskResult) o;
        return Objects.equals(processorTaskId, that.processorTaskId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(processorTaskId);
    }

    @Override
    public String toString() {
        return "ProcessorTaskResult{" +
                "processorTaskId='" + processorTaskId + '\'' +
                ", segments=" + segments +
                ", totalRecords=" + totalRecordsCount +
                '}';
    }
}
