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
package org.apache.nifi.gpfdist.metadata;

import java.util.Objects;

public class RecordProcessorId {
    private final String sinkId;
    private final String txId;
    private final int segmentId;

    public RecordProcessorId(String sinkId, String txId, int segmentId) {
        this.sinkId = sinkId;
        this.txId = txId;
        this.segmentId = segmentId;
    }

    public String getSinkId() {
        return sinkId;
    }

    public String getTxId() {
        return txId;
    }

    public int getSegmentId() {
        return segmentId;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordProcessorId that = (RecordProcessorId) o;
        return segmentId == that.segmentId && Objects.equals(sinkId, that.sinkId) && Objects.equals(txId, that.txId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sinkId, txId, segmentId);
    }

    @Override
    public String toString() {
        return "RecordProcessorId{" +
                "sinkId='" + sinkId + '\'' +
                ", txId='" + txId + '\'' +
                ", segmentId=" + segmentId +
                '}';
    }
}
