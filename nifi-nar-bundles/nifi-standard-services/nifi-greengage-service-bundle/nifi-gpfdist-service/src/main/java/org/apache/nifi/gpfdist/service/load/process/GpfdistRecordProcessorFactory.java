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
import org.apache.nifi.gpfdist.server.request.GpfdistReadableRequest;
import org.apache.nifi.gpfdist.server.request.ReadableRequest;
import org.apache.nifi.gpfdist.service.RecordProcessor;
import org.apache.nifi.gpfdist.service.load.context.WriteContext;
import org.apache.nifi.gpfdist.service.load.serialization.RecordSerializationService;
import org.apache.nifi.gpfdist.service.load.serialization.csv.CsvRecordSerializationService;
import org.apache.nifi.gpfdist.service.metadata.CsvFormatConfig;
import org.apache.nifi.gpfdist.service.metadata.DataFormatConfig;

import java.io.OutputStream;

public class GpfdistRecordProcessorFactory implements RecordProcessorFactory {
    private final DataFormatConfig dataFormatConfig;

    public GpfdistRecordProcessorFactory(final DataFormatConfig dataFormatConfig) {
        this.dataFormatConfig = dataFormatConfig;
    }

    @Override
    public RecordProcessor create(final ReadableRequest readableRequest,
                                  final WriteContext writeContext,
                                  final OutputStream outputStream) {
        return new GpfdistRecordProcessor((GpfdistReadableRequest) readableRequest,
                writeContext,
                outputStream,
                createRecordSerializationService(writeContext),
                writeContext.getLogger());
    }

    private RecordSerializationService createRecordSerializationService(WriteContext writeContext) {
        if (dataFormatConfig.getDataFormat() == DataFormat.CSV) {
            CsvFormatConfig csvFormatConfig = (CsvFormatConfig) dataFormatConfig;
            return new CsvRecordSerializationService(writeContext.getMetadata().getRecordSchema(),
                    writeContext.getMetadata().getColumnDescriptions(),
                    csvFormatConfig,
                    writeContext.getLogger());
        } else {
            throw new UnsupportedOperationException("Unsupported DataFormat: " + dataFormatConfig);
        }
    }
}
