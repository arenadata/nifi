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
package org.apache.nifi.gpfdist.service.unload.dto;

import java.util.Objects;

public class UnloadingResult {
    private final ProcessingChunkId chunkId;
    private long recordCount;
    private Throwable error;

    public UnloadingResult(ProcessingChunkId chunkId) {
        this.chunkId = chunkId;
    }

    public ProcessingChunkId getChunkId() {
        return chunkId;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public void setError(Throwable error) {
        this.error = error;
    }

    public Throwable getError() {
        return error;
    }

    public void increment() {
        recordCount++;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        UnloadingResult that = (UnloadingResult) o;
        return Objects.equals(chunkId, that.chunkId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(chunkId);
    }

    @Override
    public String toString() {
        return "UnloadingResult{" +
                "chunkId=" + chunkId +
                ", recordCount=" + recordCount +
                ", error=" + error +
                '}';
    }
}
