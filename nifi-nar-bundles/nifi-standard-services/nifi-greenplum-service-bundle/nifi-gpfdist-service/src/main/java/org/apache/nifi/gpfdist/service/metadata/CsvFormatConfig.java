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
package org.apache.nifi.gpfdist.service.metadata;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.nifi.gpfdist.metadata.DataFormat;

import static org.apache.commons.csv.CSVFormat.DEFAULT;

public class CsvFormatConfig implements DataFormatConfig {
    private static final String DEFAULT_ENCODING = "UTF-8";
    private static final CSVFormat GREENPLUM_CSV_FORMAT = DEFAULT.builder()
            .setDelimiter("|")
            .setEscape(null)
            .setIgnoreEmptyLines(false)
            .setQuote(Character.valueOf('"'))
            .setRecordSeparator("\r\n")
            .setNullString("")
            .setQuoteMode(QuoteMode.ALL_NON_NULL)
            .setSkipHeaderRecord(true)
            .build();

    public CsvFormatConfig() {
    }

    public CSVFormat getCsvFormat() {
        return GREENPLUM_CSV_FORMAT;
    }

    public String getEncoding() {
        return DEFAULT_ENCODING;
    }

    @Override
    public DataFormat getDataFormat() {
        return DataFormat.CSV;
    }
}
