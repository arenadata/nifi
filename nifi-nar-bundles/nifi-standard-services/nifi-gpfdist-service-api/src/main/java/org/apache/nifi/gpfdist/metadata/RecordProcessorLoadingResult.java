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
package org.apache.nifi.gpfdist.metadata;

import java.util.Objects;

public class RecordProcessorLoadingResult {
    private final RecordProcessorId recordProcessorId;
    private long recordCount;
    private long recordBytes;

    public RecordProcessorLoadingResult(RecordProcessorId recordProcessorId) {
        this.recordProcessorId = recordProcessorId;
    }

    public RecordProcessorId getLoadingChunkId() {
        return recordProcessorId;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public long getRecordBytes() {
        return recordBytes;
    }

    public void incrementRecordCount() {
        this.recordCount++;
    }

    public void incrementRecordBytes(long processedBytes) {
        this.recordBytes += processedBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        RecordProcessorLoadingResult that = (RecordProcessorLoadingResult) o;
        return recordCount == that.recordCount && recordBytes == that.recordBytes && Objects.equals(recordProcessorId, that.recordProcessorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordProcessorId, recordCount, recordBytes);
    }

    @Override
    public String toString() {
        return "RecordProcessorLoadingResult{" +
                "loadingChunkId=" + recordProcessorId +
                ", recordCount=" + recordCount +
                ", recordBytes=" + recordBytes +
                '}';
    }
}
