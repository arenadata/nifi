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

import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.metadata.GreengageDataType;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.gpfdist.service.util.GreengageColumnDataTypeConverter.parseArray;
import static org.apache.nifi.gpfdist.service.util.GreengageColumnDataTypeConverter.parseMap;

public abstract class AbstractGreengageCSVRecordReader implements RecordReader {
    private final boolean trimDoubleQuote;
    protected final String dateFormat;
    protected final String timeFormat;
    protected final String timestampFormat;
    protected final String timestampzFormat;
    protected final RecordSchema schema;
    protected final Map<String, ColumnDataType> dataTypes;
    protected final ComponentLog logger;

    AbstractGreengageCSVRecordReader(final RecordSchema schema,
                                     final String dateFormat,
                                     final String timeFormat,
                                     final String timestampFormat,
                                     final String timestampzFormat,
                                     final boolean trimDoubleQuote,
                                     final Map<String, ColumnDataType> dataTypes,
                                     final ComponentLog logger) {
        this.logger = logger;
        this.schema = schema;
        this.trimDoubleQuote = trimDoubleQuote;
        this.dataTypes = dataTypes;

        if (dateFormat == null || dateFormat.isEmpty()) {
            this.dateFormat = null;
        } else {
            this.dateFormat = dateFormat;
        }

        if (timeFormat == null || timeFormat.isEmpty()) {
            this.timeFormat = null;
        } else {
            this.timeFormat = timeFormat;
        }

        if (timestampFormat == null || timestampFormat.isEmpty()) {
            this.timestampFormat = null;
        } else {
            this.timestampFormat = timestampFormat;
        }

        if (timestampzFormat == null || timestampzFormat.isEmpty()) {
            this.timestampzFormat = null;
        } else {
            this.timestampzFormat = timestampzFormat;
        }
    }

    protected final Object convert(final String value, final DataType dataType, final String fieldName) {
        if (dataType == null || value == null) {
            return value;
        }

        final String trimmed;
        final RecordFieldType type = dataType.getFieldType();

        if (!trimDoubleQuote && (type.equals(RecordFieldType.STRING) || type.equals(RecordFieldType.CHOICE))) {
            trimmed = value;
        } else {
            trimmed = trim(value);
        }

        if (trimmed.isEmpty()) {
            return null;
        }
        switch (type) {
            case BOOLEAN:
                return convertBoolean(trimmed);
            case TIMESTAMP:
                return convertTimestamp(trimmed, dataType, fieldName);
            case ARRAY:
                return parseArray(trimmed,
                        ((ArrayDataType) dataType).getElementType(),
                        fieldName,
                        Optional.ofNullable(dateFormat),
                        Optional.ofNullable(timeFormat),
                        Optional.ofNullable(timestampFormat));
            case MAP:
                return parseMap(trimmed,
                        ((MapDataType) dataType).getValueType(),
                        fieldName,
                        Optional.ofNullable(dateFormat),
                        Optional.ofNullable(timeFormat),
                        Optional.ofNullable(timestampFormat));
            default:
                return DataTypeUtils.convertType(trimmed,
                        dataType,
                        Optional.ofNullable(dateFormat),
                        Optional.ofNullable(timeFormat),
                        Optional.ofNullable(timestampFormat),
                        fieldName);
        }
    }

    private boolean convertBoolean(final String value) {
        if ("1".equals(value)
                || "true".equalsIgnoreCase(value)
                || "t".equalsIgnoreCase(value)) {
            return true;
        } else if ("0".equals(value)
                || "false".equalsIgnoreCase(value)
                || "f".equalsIgnoreCase(value)) {
            return false;
        } else {
            throw new IllegalArgumentException("Failed to convert " + value + " to boolean");
        }
    }

    private Object convertTimestamp(final String value, final DataType dataType, String fieldName) {
        ColumnDataType columnDataType = Optional.ofNullable(dataTypes.get(fieldName))
                .orElseThrow(() -> new IllegalArgumentException("Failed to found data type for field " + fieldName + " in columns metadata"));
        if (columnDataType.getType() == GreengageDataType.TIMESTAMP_WITHOUT_TIME_ZONE) {
            if (DataTypeUtils.isTimestampTypeCompatible(value, timestampFormat)) {
                return DataTypeUtils.convertType(value,
                        dataType,
                        Optional.ofNullable(dateFormat),
                        Optional.ofNullable(timeFormat),
                        Optional.ofNullable(timestampFormat),
                        fieldName);
            }
            return value;
        } else if (columnDataType.getType() == GreengageDataType.TIMESTAMP_WITH_TIME_ZONE) {
            return toTimestamp(value, timestampzFormat, fieldName);
        } else {
            throw new IllegalArgumentException(String.format("Greengage data type %s of field %s does not correspond " +
                    "to record data type. Expected: %s. Actual: %s.", columnDataType, fieldName, RecordFieldType.TIMESTAMP, dataType));
        }
    }

    private Timestamp toTimestamp(final String value, String format, String fieldName) {
        if (format == null) {
            throw new IllegalArgumentException("Format for converting timestamp with timezone is required for field " + fieldName);
        }
        OffsetDateTime odt = OffsetDateTime.parse(value, DateTimeFormatter.ofPattern(timestampzFormat));
        return Timestamp.from(odt.toInstant());
    }

    protected final Object convertSimpleIfPossible(final String value, final DataType dataType, final String fieldName) {
        if (dataType == null || value == null) {
            return value;
        }

        final String trimmed;

        if (!trimDoubleQuote && dataType.getFieldType().equals(RecordFieldType.STRING)) {
            trimmed = value;
        } else {
            trimmed = trim(value);
        }

        if (trimmed.isEmpty()) {
            return null;
        }

        switch (dataType.getFieldType()) {
            case STRING:
                return value;
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BYTE:
            case CHAR:
            case SHORT:
                if (DataTypeUtils.isCompatibleDataType(trimmed, dataType)) {
                    return DataTypeUtils.convertType(trimmed,
                            dataType,
                            Optional.ofNullable(dateFormat),
                            Optional.ofNullable(timeFormat),
                            Optional.ofNullable(timestampFormat),
                            fieldName);
                }
                break;
            case BOOLEAN:
                return convertBoolean(trimmed);
            case DATE:
                if (DataTypeUtils.isDateTypeCompatible(trimmed, dateFormat)) {
                    return DataTypeUtils.convertType(trimmed,
                            dataType,
                            Optional.ofNullable(dateFormat),
                            Optional.ofNullable(timeFormat),
                            Optional.ofNullable(timestampFormat),
                            fieldName);
                }
                break;
            case TIME:
                if (DataTypeUtils.isTimeTypeCompatible(trimmed, timeFormat)) {
                    return DataTypeUtils.convertType(trimmed,
                            dataType,
                            Optional.ofNullable(dateFormat),
                            Optional.ofNullable(timeFormat),
                            Optional.ofNullable(timestampFormat),
                            fieldName);
                }
                break;
            case TIMESTAMP:
                return convertTimestamp(trimmed, dataType, fieldName);
        }

        return value;
    }

    private String trim(String value) {
        return (value.length() > 1) && value.startsWith("\"") && value.endsWith("\"") ? value.substring(1, value.length() - 1) : value;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }
}
