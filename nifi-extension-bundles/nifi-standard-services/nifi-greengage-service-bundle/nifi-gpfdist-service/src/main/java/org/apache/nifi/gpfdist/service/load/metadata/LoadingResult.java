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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LoadingResult {
    private final AtomicInteger segmentsCount = new AtomicInteger();
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final Map<Integer, LoadingSegmentResult> segmentResults = new ConcurrentHashMap<>();

    public AtomicInteger getSegmentsCount() {
        return segmentsCount;
    }

    public AtomicReference<Throwable> getError() {
        return error;
    }

    public Map<Integer, LoadingSegmentResult> getSegmentResults() {
        return segmentResults;
    }

    @Override
    public String toString() {
        return "LoadingResult{" +
                "segmentsCount=" + segmentsCount +
                ", segmentResults=" + segmentResults +
                getErrorIfNeeded() +
                '}';
    }

    private String getErrorIfNeeded() {
        if (error.get() != null) {
            return ", error=" + error.get().getMessage();
        }
        return "";
    }
}
