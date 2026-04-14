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
package org.apache.nifi.gpfdist.service.unload.metadata;

import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.ContextId;
import org.apache.nifi.gpfdist.metadata.ExternalTableFormat;
import org.apache.nifi.gpfdist.metadata.TableDescription;
import org.apache.nifi.gpfdist.service.metadata.BaseGpfdistMetadata;

import java.util.List;

public class GpfdistUnloadMetadata extends BaseGpfdistMetadata {
    private final ContextId contextId;
    private final String processorTaskId;
    private final int globalWorkerIndex;

    public GpfdistUnloadMetadata(final String externalTable,
                                 final TableDescription tableMetadata,
                                 final List<ColumnDescription> columns,
                                 final ExternalTableFormat externalTableFormatConfig,
                                 final String gpfdistLocation,
                                 final ContextId contextId,
                                 final String processorTaskId,
                                 int globalWorkerIndex) {
        super(tableMetadata, columns, externalTable, externalTableFormatConfig, gpfdistLocation);
        this.contextId = contextId;
        this.processorTaskId = processorTaskId;
        this.globalWorkerIndex = globalWorkerIndex;
    }

    public ContextId getContextId() {
        return contextId;
    }

    public int getGlobalWorkerIndex() {
        return globalWorkerIndex;
    }

    public String getProcessorTaskId() {
        return processorTaskId;
    }

    @Override
    public String toString() {
        return "GpfdistUnloadMetadata{" +
                "contextId=" + contextId +
                ", processorTaskId='" + processorTaskId + '\'' +
                ", globalWorkerIndex=" + globalWorkerIndex +
                ", tableMetadata=" + tableMetadata +
                ", columnDescriptions=" + columnDescriptions +
                ", externalTable='" + externalTable + '\'' +
                ", externalTableFormatConfig=" + externalTableFormatConfig +
                ", gpfdistLocation='" + gpfdistLocation + '\'' +
                '}';
    }
}
