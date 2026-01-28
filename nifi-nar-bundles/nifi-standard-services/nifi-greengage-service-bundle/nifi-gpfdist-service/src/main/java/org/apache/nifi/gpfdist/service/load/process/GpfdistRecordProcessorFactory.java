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
package org.apache.nifi.gpfdist.service.load.process;

import org.apache.nifi.gpfdist.metadata.DataFormat;
import org.apache.nifi.gpfdist.metadata.RecordProcessorId;
import org.apache.nifi.gpfdist.server.request.ReadableRequest;
import org.apache.nifi.gpfdist.service.RecordProcessor;
import org.apache.nifi.gpfdist.service.load.metadata.GpfdistLoadMetadata;
import org.apache.nifi.gpfdist.service.load.serialization.RecordSerializationService;
import org.apache.nifi.gpfdist.service.load.serialization.csv.CsvRecordSerializationService;
import org.apache.nifi.gpfdist.service.metadata.CsvFormatConfig;
import org.apache.nifi.gpfdist.service.metadata.DataFormatConfig;
import org.apache.nifi.logging.ComponentLog;

import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.createGpfdistFileName;

public class GpfdistRecordProcessorFactory implements RecordProcessorFactory {
    private final DataFormatConfig dataFormatConfig;

    public GpfdistRecordProcessorFactory(final DataFormatConfig dataFormatConfig) {
        this.dataFormatConfig = dataFormatConfig;
    }

    @Override
    public RecordProcessor create(final ReadableRequest readableRequest,
                                  final GpfdistLoadMetadata loadMetadata,
                                  final RecordProcessorId recordProcessorId,
                                  final GpfdistSegmentStream stream,
                                  ComponentLog logger) {
        GpfdistPacketBuilder builder = new GpfdistPacketBuilder(createGpfdistFileName(loadMetadata.getExternalTable()));
        return new GpfdistRecordProcessorNonBlocking(recordProcessorId,
                builder,
                createRecordSerializationService(loadMetadata, logger),
                stream,
                logger);
    }

    private RecordSerializationService createRecordSerializationService(GpfdistLoadMetadata loadMetadata, ComponentLog logger) {
        if (dataFormatConfig.getDataFormat() == DataFormat.CSV) {
            CsvFormatConfig csvFormatConfig = (CsvFormatConfig) dataFormatConfig;
            return new CsvRecordSerializationService(loadMetadata.getRecordSchema(),
                    loadMetadata.getColumnDescriptions(),
                    csvFormatConfig,
                    logger);
        } else {
            throw new UnsupportedOperationException("Unsupported DataFormat: " + dataFormatConfig);
        }
    }
}
