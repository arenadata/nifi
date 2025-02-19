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

package org.apache.nifi.gpfdist.service.load.serialization.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.*;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.*;

import static java.lang.String.format;

public class CsvRecordSetWriter extends AbstractRecordSetWriter implements RecordSetWriter, RawRecordWriter {
    private static final String TIMESTAMP_WITHOUT_TIME_ZONE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private static final String TIMESTAMP_WITH_TIME_ZONE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSSXXX";
    private static final String TIME_FORMAT = "HH:mm:ss.SSSSSS";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String ARRAY_VALUE_QUOTE = "\"";
    private final RecordSchema recordSchema;
    private final CSVPrinter printer;
    private final Object[] fieldValues;
    private final boolean includeHeaderLine;
    private boolean headerWritten = false;
    private String[] fieldNames;
    private final List<ColumnDescription> columnDescriptions;

    public CsvRecordSetWriter(final CSVFormat csvFormat,
                              final RecordSchema recordSchema,
                              final OutputStream out,
                              boolean includeHeaderLine,
                              final String charSet,
                              final List<ColumnDescription> columnDescriptions) throws IOException {

        super(out);
        this.recordSchema = recordSchema;
        this.includeHeaderLine = includeHeaderLine;
        this.columnDescriptions = columnDescriptions;
        final OutputStreamWriter streamWriter = new OutputStreamWriter(out, charSet);
        printer = new CSVPrinter(streamWriter, csvFormat);
        fieldValues = new Object[recordSchema.getFieldCount()];
    }

    @Override
    protected void onBeginRecordSet() {
        //we do not need to write headers, so this method do nothing
    }

    @Override
    protected Map<String, String> onFinishRecordSet() {
        return Collections.emptyMap();
    }

    @Override
    public void close() throws IOException {
        printer.close();
    }

    @Override
    public void flush() throws IOException {
        printer.flush();
    }

    private String[] getFieldNames(final Record record) {
        if (fieldNames != null) {
            return fieldNames;
        }
        final Set<String> allFields = new LinkedHashSet<>();
        allFields.addAll(recordSchema.getFieldNames());
        allFields.addAll(record.getRawFieldNames());
        fieldNames = allFields.toArray(new String[0]);
        return fieldNames;
    }

    private void includeHeaderIfNecessary(final Record record, final boolean includeOnlySchemaFields) throws IOException {
        if (headerWritten || !includeHeaderLine) {
            return;
        }

        final Object[] fieldNames;
        if (includeOnlySchemaFields) {
            fieldNames = recordSchema.getFieldNames().toArray(new Object[0]);
        } else {
            fieldNames = getFieldNames(record);
        }

        printer.printRecord(fieldNames);
        headerWritten = true;
    }

    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        includeHeaderIfNecessary(record, true);
        int i = 0;
        for (final RecordField recordField : recordSchema.getFields()) {
            ColumnDescription columnDesc = columnDescriptions.get(i);
            fieldValues[i++] = getFieldValue(record, recordField, columnDesc.getDataType());
        }
        printer.printRecord(fieldValues);
        return Collections.emptyMap();
    }

    private Object getFieldValue(final Record record, final RecordField recordField, final ColumnDataType dataType) {
        DataType fieldDataType = recordField.getDataType();
        switch (dataType.getType()) {
            case BOOLEAN:
            case MONEY:
            case JSONB:
            case UUID:
            case CHAR:
            case VARCHAR:
                return record.getAsString(recordField, fieldDataType.getFormat());
            case BIGINT:
            case DECIMAL:
            case DOUBLE_PRECISION:
            case REAL:
            case INTEGER:
            case SMALLINT:
                final Object value = record.getValue(recordField);
                if (value instanceof Number) {
                    return value;
                }
                break;
            case BIT:
                Boolean booleanValue = record.getAsBoolean(recordField.getFieldName());
                if (booleanValue == null) {
                    return null;
                }
                return booleanValue ? "1" : "0";
            case BYTEA:
                Object[] objects = record.getAsArray(recordField.getFieldName());
                if (objects == null) {
                    return null;
                }
                return toPGString(Arrays.stream(objects).toArray(Byte[]::new));
            case DATE:
                return record.getAsString(recordField, DATE_FORMAT);
            case TIME:
                return record.getAsString(recordField, TIME_FORMAT);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return record.getAsString(recordField, TIMESTAMP_WITHOUT_TIME_ZONE_FORMAT);
            case TIMESTAMP_WITH_TIME_ZONE:
                return record.getAsString(recordField, TIMESTAMP_WITH_TIME_ZONE_FORMAT);
            case ARRAY:
                Object[] arrValue = record.getAsArray(recordField.getFieldName());
                if (arrValue == null) {
                    return null;
                }
                //todo will be implement in another task
                /*return '{' +
                        Arrays.stream(arrValue)
                                .filter(Objects::nonNull)
                                .map(element -> ARRAY_VALUE_QUOTE + element + ARRAY_VALUE_QUOTE)
                                .collect(Collectors.joining(",")) +
                        '}';*/
                throw new UnsupportedOperationException("Unsupported column type " + dataType);
            case MAP:
                DataType mapDataType = recordField.getDataType();
                Object mapValue = record.getValue(recordField.getFieldName());
                if (mapValue == null) {
                    return null;
                }
                //todo will be implement in another task
                /*if (mapDataType.getFieldType() == RecordFieldType.STRING) {
                    String mapValueString = mapValue.toString().substring(1, mapValue.toString().length() - 1);
                    return Arrays.stream(mapValueString.split(","))
                            .map(val -> Arrays.stream(val.trim().split("="))
                                    .map(entry -> '"' + entry + '"')
                                    .collect(Collectors.joining("=>")))
                            .collect(Collectors.joining(", "));
                }*/
                throw new IllegalArgumentException(format("Unsupported record field type: %s for column type hstore", mapDataType.getFieldType()));
        }
        return record.getAsString(recordField, fieldDataType.getFormat());
    }

    @Override
    public WriteResult writeRawRecord(final Record record) throws IOException {
        includeHeaderIfNecessary(record, false);
        final String[] fieldNames = getFieldNames(record);
        final Object[] recordFieldValues = (fieldNames.length == this.fieldValues.length) ? this.fieldValues : new String[fieldNames.length];

        int i = 0;
        for (final String fieldName : fieldNames) {
            final Optional<RecordField> recordField = recordSchema.getField(fieldName);
            if (recordField.isPresent()) {
                recordFieldValues[i++] = record.getAsString(fieldName, recordField.get().getDataType().getFormat());
            } else {
                recordFieldValues[i++] = record.getAsString(fieldName);
            }
        }

        printer.printRecord(recordFieldValues);
        return WriteResult.of(incrementRecordCount(), Collections.emptyMap());
    }

    public static String toPGString(Byte[] buf) {
        if (buf == null) {
            return null;
        } else {
            StringBuilder stringBuilder = new StringBuilder(2 + 2 * buf.length);
            stringBuilder.append("\\x");
            appendHexString(stringBuilder, buf, 0, buf.length);
            return stringBuilder.toString();
        }
    }

    public static void appendHexString(StringBuilder sb, Byte[] buf, int offset, int length) {
        for (int i = offset; i < offset + length; ++i) {
            byte element = buf[i];
            sb.append(Character.forDigit(element >> 4 & 15, 16));
            sb.append(Character.forDigit(element & 15, 16));
        }
    }

    @Override
    public String getMimeType() {
        return "text/csv";
    }
}
