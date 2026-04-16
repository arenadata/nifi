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
import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.metadata.TableDescription;
import org.apache.nifi.gpfdist.service.GpfdistUnloadMetadataFactory;
import org.apache.nifi.gpfdist.service.metadata.ExternalTableFormatConfigFactory;
import org.apache.nifi.gpfdist.service.metadata.ExternalTableType;
import org.apache.nifi.gpfdist.service.metadata.GpfdistLocationFactory;

import java.util.List;

import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.createExternalTableName;

public class DefaultGpfdistUnloadMetadataFactory implements GpfdistUnloadMetadataFactory {
    private static final ExternalTableType EXTERNAL_TABLE_TYPE = ExternalTableType.WRITABLE;
    private final GpfdistLocationFactory gpfdistLocationFactory;
    private final ExternalTableFormatConfigFactory externalTableFormatConfigFactory;

    public DefaultGpfdistUnloadMetadataFactory(final GpfdistLocationFactory gpfdistLocationFactory,
                                               final ExternalTableFormatConfigFactory externalTableFormatConfigFactory) {
        this.gpfdistLocationFactory = gpfdistLocationFactory;
        this.externalTableFormatConfigFactory = externalTableFormatConfigFactory;
    }

    @Override
    public GpfdistMetadata create(TableDescription tableMetadata,
                                  List<ColumnDescription> columnDescriptions,
                                  ContextId contextId,
                                  String processorTaskId,
                                  int globalWorkerIndex) {
        ExternalTableFormat tableFormatConfig = externalTableFormatConfigFactory.create();
        String externalTable = createExternalTableName(EXTERNAL_TABLE_TYPE, globalWorkerIndex);
        String gpfdistLocation = gpfdistLocationFactory.create(contextId, processorTaskId, externalTable, EXTERNAL_TABLE_TYPE);
        return new GpfdistUnloadMetadata(externalTable,
                tableMetadata,
                columnDescriptions,
                tableFormatConfig,
                gpfdistLocation,
                contextId,
                processorTaskId,
                globalWorkerIndex);
    }
}
