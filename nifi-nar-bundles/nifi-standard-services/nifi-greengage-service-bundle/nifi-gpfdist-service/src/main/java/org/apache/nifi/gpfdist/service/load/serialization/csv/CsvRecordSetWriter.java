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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RawRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.gpfdist.metadata.GreengageDataType.ARRAY;
import static org.apache.nifi.gpfdist.metadata.GreengageDataType.BYTEA;
import static org.apache.nifi.gpfdist.metadata.GreengageDataType.MAP;

public class CsvRecordSetWriter extends AbstractRecordSetWriter implements RecordSetWriter, RawRecordWriter {
    static final String TIMESTAMP_WITHOUT_TIME_ZONE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    static final String TIMESTAMP_WITH_TIME_ZONE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSSXXX";
    static final String TIME_FORMAT = "HH:mm:ss.SSSSSS";
    static final String DATE_FORMAT = "yyyy-MM-dd";

    private static final String MAP_TYPE_VALUE_SEPARATOR = "=>";
    private static final char VALUE_QUOTE = '"';
    private static final String ELEMENT_DELIMITER = ", ";
    private static final String AARRAY_VALUE_DELIMITER = ",";
    private final RecordSchema recordSchema;
    private final CSVPrinter printer;
    private final Object[] fieldValues;
    private final boolean includeHeaderLine;
    private boolean headerWritten = false;
    private String[] fieldNames;
    private final List<ColumnDescription> columnDescriptions;
    private final ComponentLog logger;

    public CsvRecordSetWriter(final CSVFormat csvFormat,
                              final RecordSchema recordSchema,
                              final OutputStream out,
                              boolean includeHeaderLine,
                              final String charSet,
                              final List<ColumnDescription> columnDescriptions,
                              ComponentLog logger) throws IOException {

        super(out);
        this.recordSchema = recordSchema;
        this.includeHeaderLine = includeHeaderLine;
        this.columnDescriptions = columnDescriptions;
        final OutputStreamWriter streamWriter = new OutputStreamWriter(out, charSet);
        printer = new CSVPrinter(streamWriter, csvFormat);
        fieldValues = new Object[recordSchema.getFieldCount()];
        this.logger = logger;
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
    public Map<String, String> writeRecord(final Record record) {
        try {
            includeHeaderIfNecessary(record, true);
            int i = 0;
            for (final RecordField recordField : recordSchema.getFields()) {
                ColumnDescription columnDesc = columnDescriptions.get(i);
                fieldValues[i++] = getFieldValue(record, recordField, columnDesc.getDataType());
            }
            printer.printRecord(fieldValues);
        } catch (Exception e) {
            String errMsg = "Failed to write record: " + e.getMessage();
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
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
            case ENUM:
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
                DataType byteDataType = recordField.getDataType();
                if (byteDataType.getFieldType() == RecordFieldType.ARRAY) {
                    Object[] objects = record.getAsArray(recordField.getFieldName());
                    if (objects == null) {
                        return null;
                    }
                    return toPGString(Arrays.stream(objects).toArray(Byte[]::new));
                } else if (byteDataType.getFieldType() == RecordFieldType.STRING) {
                    return record.getAsString(recordField.getFieldName());
                } else {
                    throw new IllegalArgumentException("Unsupported field type: " + byteDataType + " for column type " + BYTEA);
                }
            case DATE:
                return record.getAsString(recordField, DATE_FORMAT);
            case TIME:
                return record.getAsString(recordField, TIME_FORMAT);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return record.getAsString(recordField, TIMESTAMP_WITHOUT_TIME_ZONE_FORMAT);
            case TIMESTAMP_WITH_TIME_ZONE:
                return record.getAsString(recordField, TIMESTAMP_WITH_TIME_ZONE_FORMAT);
            case ARRAY:
                DataType arrayDataType = recordField.getDataType();
                if (arrayDataType.getFieldType() == RecordFieldType.ARRAY) {
                    return getArrayValue(record, recordField, arrayDataType);
                } else if (arrayDataType.getFieldType() == RecordFieldType.STRING) {
                    return record.getAsString(recordField.getFieldName());
                }
                throw new IllegalArgumentException("Unsupported field type: " + arrayDataType + " for column type " + ARRAY);
            case MAP:
                RecordFieldType mapDataType = recordField.getDataType().getFieldType();
                Object mapValue = record.getValue(recordField.getFieldName());
                if (mapValue == null) {
                    return null;
                }
                if (mapDataType == RecordFieldType.STRING) {
                    return getMapAsString(mapValue);
                } else if (mapDataType == RecordFieldType.MAP) {
                    return getMapValue((Map<?, ?>) mapValue);
                }
                throw new IllegalArgumentException("Unsupported record field type: " + mapDataType + " for column type " + MAP);
        }
        return record.getAsString(recordField, fieldDataType.getFormat());
    }

    private String getArrayValue(Record record, RecordField recordField, DataType arrayDataType) {
        Object[] array = record.getAsArray(recordField.getFieldName());
        if (array == null) {
            return null;
        }
        DataType elementType = ((ArrayDataType) arrayDataType).getElementType();
        switch (elementType.getFieldType()) {
            case STRING:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BIGINT:
            case BOOLEAN:
            case SHORT:
                return getValueFromObjectArray(array);
            case BYTE:
                return getValueFromByteArray(array);
            default:
                throw new IllegalArgumentException("Unsupported field array element type: " + elementType + " for column type " + ARRAY);
        }
    }

    private String getValueFromByteArray(Object[] array) {
        byte[] newByteArray = new byte[array.length];
        for (int i = 0; i < array.length; i++) {
            newByteArray[i] = (Byte) array[i];
        }
        String arrStr = new String(newByteArray, StandardCharsets.UTF_8);
        String mapValueString = getValueWithoutBraces(arrStr);
        return getValueFromObjectArray(Arrays.stream(mapValueString.split(AARRAY_VALUE_DELIMITER))
                .map(String::trim)
                .toArray(String[]::new));
    }

    private String getValueFromObjectArray(Object[] array) {
        return '{' +
                Arrays.stream(array)
                        .filter(Objects::nonNull)
                        .map(value -> VALUE_QUOTE + value.toString() + VALUE_QUOTE)
                        .collect(Collectors.joining(ELEMENT_DELIMITER)) +
                '}';
    }

    private String getMapAsString(Object mapValue) {
        return Arrays.stream(getValueWithoutBraces(mapValue.toString()).split(AARRAY_VALUE_DELIMITER))
                .map(val -> Arrays.stream(val.trim().split("="))
                        .map(entry -> VALUE_QUOTE + entry + VALUE_QUOTE)
                        .collect(Collectors.joining(MAP_TYPE_VALUE_SEPARATOR)))
                .collect(Collectors.joining(ELEMENT_DELIMITER));
    }

    private String getMapValue(Map<?, ?> mapValue) {
        //support only map with v keys and values
        return mapValue.entrySet().stream()
                .map(entry -> VALUE_QUOTE + entry.getKey().toString() + VALUE_QUOTE
                        + MAP_TYPE_VALUE_SEPARATOR + VALUE_QUOTE + entry.getValue().toString() + VALUE_QUOTE)
                .collect(Collectors.joining(ELEMENT_DELIMITER));
    }

    private String getValueWithoutBraces(String arrStr) {
        return arrStr.substring(1, arrStr.length() - 1);
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
