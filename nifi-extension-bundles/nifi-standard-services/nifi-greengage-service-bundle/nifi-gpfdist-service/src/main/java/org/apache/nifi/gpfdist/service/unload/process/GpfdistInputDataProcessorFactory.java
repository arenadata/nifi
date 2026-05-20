/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.gpfdist.service.unload.process;

import org.apache.nifi.gpfdist.metadata.DataFormat;
import org.apache.nifi.gpfdist.service.metadata.DataFormatConfig;
import org.apache.nifi.gpfdist.service.unload.context.ReadContext;
import org.apache.nifi.gpfdist.service.unload.dto.ProcessingChunkId;
import org.apache.nifi.logging.ComponentLog;

public class GpfdistInputDataProcessorFactory implements InputDataProcessorFactory {

    private final DataFormatConfig dataFormatConfig;
    private final ComponentLog logger;

    public GpfdistInputDataProcessorFactory(DataFormatConfig dataFormatConfig, ComponentLog logger) {
        this.dataFormatConfig = dataFormatConfig;
        this.logger = logger;
    }

    @Override
    public InputDataProcessor create(ReadContext readContext,
                                     ProcessingChunkId metadata,
                                     RecordProcessingService recordProcessingService) {
        if (dataFormatConfig.getDataFormat() == DataFormat.CSV) {
            return new GpfdistCsvDataProcessor(dataFormatConfig,
                    readContext.getRecordSchema(),
                    readContext.getDataTypes(),
                    recordProcessingService,
                    metadata,
                    logger);
        } else {
            throw new UnsupportedOperationException("Unsupported data format: " + dataFormatConfig.getDataFormat());
        }
    }
}
