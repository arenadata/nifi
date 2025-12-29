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

import java.util.List;
import java.util.Objects;

public class SegmentResult {
    private final int segmentId;
    private final List<TransactionResult> transactions;
    private final long recordsCount;

    public SegmentResult(int segmentId,
                         List<TransactionResult> transactions,
                         long totalRecords) {
        this.segmentId = segmentId;
        this.transactions = transactions;
        this.recordsCount = totalRecords;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SegmentResult that = (SegmentResult) o;
        return segmentId == that.segmentId && Objects.equals(transactions, that.transactions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentId, transactions);
    }

    @Override
    public String toString() {
        return "SegmentResult{" +
                "segmentId=" + segmentId +
                ", transactions=" + transactions +
                ", recordsCount=" + recordsCount +
                '}';
    }
}
