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

import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.nifi.gpfdist.service.util.GreengageColumnDataTypeConverter.parseArray;
import static org.apache.nifi.gpfdist.service.util.GreengageColumnDataTypeConverter.parseMap;

abstract public class AbstractGreengageCSVRecordReader implements RecordReader {
    private final boolean trimDoubleQuote;
    protected final Supplier<DateFormat> LAZY_DATE_FORMAT;
    protected final Supplier<DateFormat> LAZY_TIME_FORMAT;
    protected final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;
    protected final String dateFormat;
    protected final String timeFormat;
    protected final String timestampFormat;
    protected final String timestampzFormat;
    protected final RecordSchema schema;
    protected final Map<String, ColumnDataType> dataTypes;
    protected final ComponentLog logger;
    private static final String FLEXIBLE_FRACTION_FORMAT = "[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]";
    private static final DateTimeFormatter FLEXIBLE_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss" + FLEXIBLE_FRACTION_FORMAT);
    private static final DateTimeFormatter FLEXIBLE_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss" + FLEXIBLE_FRACTION_FORMAT);
    private static final DateTimeFormatter FLEXIBLE_TIMESTAMP_WITH_TIME_ZONE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss" + FLEXIBLE_FRACTION_FORMAT + "X");

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
            LAZY_DATE_FORMAT = null;
        } else {
            this.dateFormat = dateFormat;
            LAZY_DATE_FORMAT = () -> DataTypeUtils.getDateFormat(dateFormat);
        }

        if (timeFormat == null || timeFormat.isEmpty()) {
            this.timeFormat = null;
            LAZY_TIME_FORMAT = null;
        } else {
            this.timeFormat = timeFormat;
            LAZY_TIME_FORMAT = () -> DataTypeUtils.getDateFormat(timeFormat);
        }

        if (timestampFormat == null || timestampFormat.isEmpty()) {
            this.timestampFormat = null;
            LAZY_TIMESTAMP_FORMAT = null;
        } else {
            this.timestampFormat = timestampFormat;
            LAZY_TIMESTAMP_FORMAT = () -> DataTypeUtils.getDateFormat(timestampFormat);
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
            case TIME:
                return convertTime(trimmed);
            case TIMESTAMP:
                return convertTimestamp(trimmed, dataType, fieldName);
            case ARRAY:
                return parseArray(trimmed, ((ArrayDataType) dataType).getElementType(), fieldName, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT);
            case MAP:
                return parseMap(trimmed, ((MapDataType) dataType).getValueType(), fieldName, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT);
            default:
                return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
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
            try {
                return Timestamp.valueOf(LocalDateTime.parse(value, FLEXIBLE_TIMESTAMP_FORMATTER));
            } catch (final DateTimeParseException e) {
                if (DataTypeUtils.isTimestampTypeCompatible(value, timestampFormat)) {
                    return DataTypeUtils.convertType(value, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                }
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
        OffsetDateTime odt = OffsetDateTime.parse(value, FLEXIBLE_TIMESTAMP_WITH_TIME_ZONE_FORMATTER);
        return Timestamp.from(odt.toInstant());
    }

    private Time convertTime(final String value) {
        final LocalTime localTime = LocalTime.parse(value, FLEXIBLE_TIME_FORMATTER);
        final Time time = Time.valueOf(localTime);
        time.setTime(time.getTime() + localTime.getNano() / 1_000_000L);
        return time;
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
                    return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                }
                break;
            case BOOLEAN:
                return convertBoolean(trimmed);
            case DATE:
                if (DataTypeUtils.isDateTypeCompatible(trimmed, dateFormat)) {
                    return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                }
                break;
            case TIME:
                try {
                    return convertTime(trimmed);
                } catch (DateTimeParseException e) {
                    if (DataTypeUtils.isTimeTypeCompatible(trimmed, timeFormat)) {
                        return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                    }
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
