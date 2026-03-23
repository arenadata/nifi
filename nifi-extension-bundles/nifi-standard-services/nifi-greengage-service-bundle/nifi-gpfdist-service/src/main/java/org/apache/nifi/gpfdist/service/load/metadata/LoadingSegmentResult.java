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
package org.apache.nifi.gpfdist.service.load.metadata;

import java.util.Objects;

public class LoadingSegmentResult {
    private final int segmentId;
    private final long loadedRecords;
    private final long loadedBytes;

    public LoadingSegmentResult(int segmentId, long loadedRecords, long loadedBytes) {
        this.segmentId = segmentId;
        this.loadedRecords = loadedRecords;
        this.loadedBytes = loadedBytes;
    }

    public int getSegmentId() {
        return segmentId;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        LoadingSegmentResult that = (LoadingSegmentResult) o;
        return segmentId == that.segmentId && loadedRecords == that.loadedRecords && loadedBytes == that.loadedBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentId, loadedRecords, loadedBytes);
    }

    @Override
    public String toString() {
        return "LoadingSegmentResult{" +
                "segmentId=" + segmentId +
                ", loadedRecords=" + loadedRecords +
                ", loadedBytes=" + loadedBytes +
                '}';
    }
}
