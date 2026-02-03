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
package org.apache.nifi.gpfdist.service.metadata;

import org.apache.nifi.gpfdist.metadata.DataFormat;
import org.apache.nifi.gpfdist.metadata.ExternalTableFormat;

public class DefaultExternalTableFormatConfigFactory
        implements ExternalTableFormatConfigFactory {
    private final DataFormatConfig dataFormatConfig;

    public DefaultExternalTableFormatConfigFactory(final DataFormatConfig dataFormatConfig) {
        this.dataFormatConfig = dataFormatConfig;
    }

    @Override
    public ExternalTableFormat create() {
        if (dataFormatConfig.getDataFormat() == DataFormat.CSV) {
            CsvFormatConfig csvFormatConfig = (CsvFormatConfig) dataFormatConfig;
            return new DefaultExternalTableFormat(csvFormatConfig.getCsvFormat().getDelimiterString(),
                    csvFormatConfig.getEncoding(),
                    csvFormatConfig.getCsvFormat().getNullString(),
                    csvFormatConfig.getDataFormat());
        } else {
            throw new UnsupportedOperationException("Unsupported DataFormat: " + dataFormatConfig);
        }
    }
}
