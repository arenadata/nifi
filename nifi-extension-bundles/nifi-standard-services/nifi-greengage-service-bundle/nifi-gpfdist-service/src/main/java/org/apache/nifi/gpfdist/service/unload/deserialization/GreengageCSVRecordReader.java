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

package org.apache.nifi.gpfdist.service.unload.deserialization;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GreengageCSVRecordReader extends AbstractGreengageCSVRecordReader {
    private final CSVParser csvParser;
    private final List<RecordField> recordFields;

    public GreengageCSVRecordReader(final InputStream in,
                                    final ComponentLog logger,
                                    final RecordSchema schema,
                                    final CSVFormat csvFormat,
                                    final String dateFormat,
                                    final String timeFormat,
                                    final String timestampFormat,
                                    final String timestampzFormat,
                                    final String encoding,
                                    final boolean trimDoubleQuote,
                                    final Map<String, ColumnDataType> dataTypes) throws IOException {
        super(schema, dateFormat, timeFormat, timestampFormat, timestampzFormat, trimDoubleQuote, dataTypes, logger);
        final Reader reader = new InputStreamReader(BOMInputStream.builder()
                .setInputStream(in)
                .get(), encoding);
        csvParser = new CSVParser(reader, csvFormat);
        recordFields = schema.getFields();
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws MalformedRecordException {

        try {
            final RecordSchema schema = getSchema();
            final int numFieldNames = recordFields.size();
            for (final CSVRecord csvRecord : csvParser) {
                final Map<String, Object> values = new LinkedHashMap<>(recordFields.size() * 2);
                for (int i = 0; i < csvRecord.size(); i++) {
                    final String rawValue = csvRecord.get(i);
                    final String rawFieldName;
                    final DataType dataType;
                    if (i >= numFieldNames) {
                        if (!dropUnknownFields) {
                            values.put("unknown_field_index_" + i, rawValue);
                        }
                        continue;
                    } else {
                        final RecordField recordField = recordFields.get(i);
                        rawFieldName = recordField.getFieldName();
                        dataType = recordField.getDataType();
                    }
                    final Object value;
                    if (coerceTypes) {
                        value = convert(rawValue, dataType, rawFieldName);
                    } else {
                        value = convertSimpleIfPossible(rawValue, dataType, rawFieldName);
                    }
                    values.put(rawFieldName, value);
                }
                return new MapRecord(schema, values, coerceTypes, dropUnknownFields);
            }
        } catch (Exception e) {
            throw new MalformedRecordException("Error while getting next record", e);
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        csvParser.close();
    }
}
