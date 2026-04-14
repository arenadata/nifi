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
package org.apache.nifi.gpfdist.service.load.metadata.factory;

import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.ContextId;
import org.apache.nifi.gpfdist.metadata.ExternalTableFormat;
import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.metadata.TableDescription;
import org.apache.nifi.gpfdist.service.GpfdistLoadMetadataFactory;
import org.apache.nifi.gpfdist.service.load.metadata.GpfdistLoadMetadata;
import org.apache.nifi.gpfdist.service.metadata.ExternalTableFormatConfigFactory;
import org.apache.nifi.gpfdist.service.metadata.ExternalTableType;
import org.apache.nifi.gpfdist.service.metadata.GpfdistLocationFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.List;

import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.createExternalTableName;

public class DefaultGpfdistLoadMetadataFactory implements GpfdistLoadMetadataFactory {
    private static final ExternalTableType EXTERNAL_TABLE_TYPE = ExternalTableType.READABLE;
    private final GpfdistLocationFactory gpfdistLocationFactory;
    private final ExternalTableFormatConfigFactory externalTableFormatConfigFactory;

    public DefaultGpfdistLoadMetadataFactory(final GpfdistLocationFactory gpfdistLocationFactory,
                                             final ExternalTableFormatConfigFactory externalTableFormatConfigFactory) {
        this.gpfdistLocationFactory = gpfdistLocationFactory;
        this.externalTableFormatConfigFactory = externalTableFormatConfigFactory;
    }

    @Override
    public GpfdistMetadata create(ContextId contextId,
                                  String sinkId,
                                  TableDescription tableMetadata,
                                  List<ColumnDescription> columnDescriptions,
                                  RecordSchema recordSchema) {
        ExternalTableFormat tableFormatConfig = externalTableFormatConfigFactory.create();
        String externalTable = createExternalTableName(EXTERNAL_TABLE_TYPE);
        String gpfdistLocation = gpfdistLocationFactory.create(contextId, sinkId, externalTable, EXTERNAL_TABLE_TYPE);
        if (recordSchema.getFieldCount() != columnDescriptions.size()) {
            throw new IllegalArgumentException("Schema does not match target column count");
        }
        return new GpfdistLoadMetadata(externalTable,
                tableMetadata,
                columnDescriptions,
                tableFormatConfig,
                gpfdistLocation,
                recordSchema);
    }
}
