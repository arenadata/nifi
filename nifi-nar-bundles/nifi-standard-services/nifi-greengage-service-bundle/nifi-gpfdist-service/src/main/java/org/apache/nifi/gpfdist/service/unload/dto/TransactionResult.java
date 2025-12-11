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

public class TransactionResult {
    private final String txId;
    private final long recordsCount;

    public TransactionResult(String txId, long recordsCount) {
        this.txId = txId;
        this.recordsCount = recordsCount;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        TransactionResult that = (TransactionResult) o;
        return recordsCount == that.recordsCount && Objects.equals(txId, that.txId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(txId, recordsCount);
    }

    @Override
    public String toString() {
        return "TransactionResult{" +
                "txId='" + txId + '\'' +
                ", recordsCount=" + recordsCount +
                '}';
    }
}
