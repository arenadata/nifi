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

import java.util.Objects;

public class ProcessingChunkId {
    private final String processorTaskId;
    private final String txId;
    private final int segmentId;

    public ProcessingChunkId(String processorTaskId, String txId, int segmentId) {
        this.processorTaskId = processorTaskId;
        this.txId = txId;
        this.segmentId = segmentId;
    }

    public String getProcessorTaskId() {
        return processorTaskId;
    }

    public String getTxId() {
        return txId;
    }

    public int getSegmentId() {
        return segmentId;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ProcessingChunkId that = (ProcessingChunkId) o;
        return segmentId == that.segmentId && Objects.equals(processorTaskId, that.processorTaskId) && Objects.equals(txId, that.txId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processorTaskId, txId, segmentId);
    }

    @Override
    public String toString() {
        return "DataProcessorMetadata{" +
                "processorTaskId='" + processorTaskId + '\'' +
                ", txId='" + txId + '\'' +
                ", segmentId=" + segmentId +
                '}';
    }
}
