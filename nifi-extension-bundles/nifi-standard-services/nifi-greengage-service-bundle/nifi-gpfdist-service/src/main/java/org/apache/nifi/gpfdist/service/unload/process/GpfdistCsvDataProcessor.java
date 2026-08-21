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


import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.service.metadata.CsvFormatConfig;
import org.apache.nifi.gpfdist.service.metadata.DataFormatConfig;
import org.apache.nifi.gpfdist.service.unload.deserialization.GreengageCSVRecordReader;
import org.apache.nifi.gpfdist.service.unload.dto.ProcessingChunkId;
import org.apache.nifi.gpfdist.service.unload.dto.UnloadingResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

public class GpfdistCsvDataProcessor implements InputDataProcessor {
    static final String TIMESTAMP_WITHOUT_TIME_ZONE_FORMAT = "yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]";
    static final String TIMESTAMP_WITH_TIME_ZONE_FORMAT = "yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]X";
    static final String TIME_FORMAT = "HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]";
    static final String DATE_FORMAT = "yyyy-MM-dd";
    private final RecordSchema schema;
    private final Map<String, ColumnDataType> dataTypes;
    private final RecordProcessingService recordProcessingService;
    private final CsvFormatConfig csvFormatConfig;
    private final UnloadingResult unloadingResult;
    private final ComponentLog logger;

    public GpfdistCsvDataProcessor(DataFormatConfig dataFormatConfig,
                                   RecordSchema schema,
                                   Map<String, ColumnDataType> dataTypes,
                                   RecordProcessingService recordProcessingService,
                                   ProcessingChunkId chunkId,
                                   ComponentLog logger) {
        this.schema = schema;
        this.dataTypes = dataTypes;
        this.recordProcessingService = recordProcessingService;
        this.csvFormatConfig = (CsvFormatConfig) dataFormatConfig;
        this.logger = logger;
        unloadingResult = new UnloadingResult(chunkId);
    }

    @Override
    public void process(InputStream dataStream) throws IOException, MalformedRecordException {
        try (RecordReader recordReader = new GreengageCSVRecordReader(
                dataStream,
                logger,
                schema,
                csvFormatConfig.getCsvFormat(),
                DATE_FORMAT,
                TIME_FORMAT,
                TIMESTAMP_WITHOUT_TIME_ZONE_FORMAT,
                TIMESTAMP_WITH_TIME_ZONE_FORMAT,
                csvFormatConfig.getEncoding(),
                false,
                dataTypes)) {
            Record record;
            while ((record = recordReader.nextRecord()) != null) {
                recordProcessingService.put(record);
                unloadingResult.increment();
            }
        }
    }

    @Override
    public UnloadingResult getResult() {
        return unloadingResult;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GpfdistCsvDataProcessor that = (GpfdistCsvDataProcessor) o;
        return Objects.equals(schema, that.schema)
                && Objects.equals(dataTypes, that.dataTypes)
                && Objects.equals(recordProcessingService, that.recordProcessingService)
                && Objects.equals(csvFormatConfig, that.csvFormatConfig)
                && Objects.equals(unloadingResult, that.unloadingResult);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, dataTypes, recordProcessingService, csvFormatConfig, unloadingResult);
    }

    @Override
    public String toString() {
        return "GpfdistCsvDataProcessor{" +
                "schema=" + schema +
                ", dataTypes=" + dataTypes +
                ", recordProcessingService=" + recordProcessingService +
                ", csvFormatConfig=" + csvFormatConfig +
                ", unloadingResult=" + unloadingResult +
                '}';
    }
}
