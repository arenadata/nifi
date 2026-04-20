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
package org.apache.nifi.gpfdist.service.util;

import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.service.datatype.ArrayDataType;
import org.apache.nifi.gpfdist.service.datatype.DecimalDataType;
import org.apache.nifi.gpfdist.service.datatype.MapDataType;
import org.apache.nifi.gpfdist.service.datatype.MoneyDataType;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class GreengageColumnDataTypeConverter {
    public static RecordSchema convert(List<ColumnDescription> columns) {
        List<RecordField> fields = new ArrayList<>();

        for (ColumnDescription columnDesc : columns) {
            String columnName = columnDesc.getName();
            ColumnDataType columnType = columnDesc.getDataType();
            fields.add(new RecordField(columnName, getDataType(columnType), columnDesc.isNullable()));
        }
        return new SimpleRecordSchema(fields);
    }

    private static DataType getDataType(ColumnDataType columnType) {
        switch (columnType.getType()) {
            case MONEY:
                MoneyDataType moneyDataType = (MoneyDataType) columnType;
                return RecordFieldType.DECIMAL.getDecimalDataType(moneyDataType.getPrecision(), moneyDataType.getScale());
            case DECIMAL:
                DecimalDataType decimalDataType = (DecimalDataType) columnType;
                return RecordFieldType.DECIMAL.getDecimalDataType(decimalDataType.getPrecision(), decimalDataType.getScale());
            case ARRAY:
                ColumnDataType columnElementType = ((ArrayDataType) columnType).getElementType();
                DataType elementDataType = mapColumnDataType(columnElementType).getDataType();
                return RecordFieldType.ARRAY.getArrayDataType(elementDataType);
            case MAP:
                MapDataType mapDataType = (MapDataType) columnType;
                ColumnDataType valueColumnType = mapDataType.getValueDataType();
                DataType valueDataType = mapColumnDataType(valueColumnType).getDataType();
                return RecordFieldType.MAP.getMapDataType(valueDataType);
            default:
                return mapColumnDataType(columnType).getDataType();
        }
    }

    private static RecordFieldType mapColumnDataType(ColumnDataType columnType) {
        switch (columnType.getType()) {
            case BOOLEAN:
            case BIT:
                return RecordFieldType.BOOLEAN;
            case DECIMAL:
                return RecordFieldType.DECIMAL;
            case BIGINT:
                return RecordFieldType.LONG;
            case INTEGER:
            case SMALLINT:
                return RecordFieldType.INT;
            case REAL:
                return RecordFieldType.FLOAT;
            case DOUBLE_PRECISION:
                return RecordFieldType.DOUBLE;
            case DATE:
                return RecordFieldType.DATE;
            case TIME:
                return RecordFieldType.TIME;
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return RecordFieldType.TIMESTAMP;
            case ARRAY:
                return RecordFieldType.ARRAY;
            case MAP:
                return RecordFieldType.MAP;
            case CHAR:
            case VARCHAR:
            case BYTEA:
            case JSONB:
            case UUID:
            default:
                return RecordFieldType.STRING;
        }
    }

    public static Object[] parseArray(final String value,
                                      final DataType elementDataType,
                                      final String fieldName,
                                      Supplier<DateFormat> dateFormat,
                                      Supplier<DateFormat> timeFormat,
                                      Supplier<DateFormat> timestampFormat) {
        if (value == null) {
            return null;
        }
        String trimmedValue = value.trim();
        if (trimmedValue.isEmpty()) {
            return new Object[0];
        }
        if (trimmedValue.startsWith("[") && trimmedValue.endsWith("]")) {
            trimmedValue = trimmedValue.substring(1, trimmedValue.length() - 1).trim();
        }
        if (trimmedValue.startsWith("{") && trimmedValue.endsWith("}")) {
            trimmedValue = trimmedValue.substring(1, trimmedValue.length() - 1);
        }
        trimmedValue = trimmedValue.trim();
        if (trimmedValue.isEmpty()) {
            return new Object[0];
        }
        List<String> rawElements = splitRespectingQuotes(trimmedValue);
        List<Object> result = new ArrayList<>(rawElements.size());
        for (String rawElement : rawElements) {
            String token = rawElement.trim();
            if (token.isEmpty() || token.equalsIgnoreCase("NULL")) {
                result.add(null);
                continue;
            }
            Object converted = DataTypeUtils.convertType(
                    token,
                    elementDataType,
                    dateFormat,
                    timeFormat,
                    timestampFormat,
                    fieldName);
            result.add(converted);
        }
        return result.toArray();
    }

    public static Map<String, Object> parseMap(final String value,
                                               final DataType valueDataType,
                                               final String fieldName,
                                               final Supplier<DateFormat> dateFormat,
                                               final Supplier<DateFormat> timeFormat,
                                               final Supplier<DateFormat> timestampFormat) {
        if (value == null) {
            return null;
        }
        String trimmedValue = value.trim();
        if (trimmedValue.isEmpty()) {
            return Collections.emptyMap();
        }
        if (trimmedValue.startsWith("[") && trimmedValue.endsWith("]")) {
            trimmedValue = trimmedValue.substring(1, trimmedValue.length() - 1).trim();
        }
        if (trimmedValue.isEmpty()) {
            return Collections.emptyMap();
        }
        List<String> pairs = splitRespectingQuotes(trimmedValue);
        Map<String, Object> result = new LinkedHashMap<>(pairs.size());
        for (String pair : pairs) {
            String entry = pair.trim();
            if (entry.isEmpty()) {
                continue;
            }
            String key;
            String rawValPart;

            int firstQuote = entry.indexOf('"');
            int arrowIndex;

            if (firstQuote >= 0) {
                int secondQuote = entry.indexOf('"', firstQuote + 1);
                if (secondQuote < 0) {
                    arrowIndex = entry.indexOf("=>", firstQuote + 1);
                    if (arrowIndex < 0) {
                        continue;
                    }
                    key = entry.substring(firstQuote + 1, arrowIndex).trim();
                } else {
                    key = entry.substring(firstQuote + 1, secondQuote);
                    arrowIndex = entry.indexOf("=>", secondQuote + 1);
                }
            } else {
                arrowIndex = entry.indexOf("=>");
                if (arrowIndex < 0) {
                    continue;
                }
                key = entry.substring(0, arrowIndex).trim();
                if (key.startsWith("\"") && key.endsWith("\"") && key.length() > 1) {
                    key = key.substring(1, key.length() - 1);
                }
            }
            if (arrowIndex < 0) {
                continue;
            }
            rawValPart = entry.substring(arrowIndex + 2).trim();
            String rawValue = rawValPart;
            if (rawValue.startsWith("\"")) {
                int lastQuote = rawValue.lastIndexOf('"');
                if (lastQuote > 0) {
                    rawValue = rawValue.substring(1, lastQuote);
                } else {
                    rawValue = rawValue.substring(1);
                }
            }
            rawValue = rawValue.trim();
            if (rawValue.isEmpty() || rawValue.equalsIgnoreCase("NULL")) {
                result.put(key, null);
                continue;
            }
            Object converted;
            try {
                converted = DataTypeUtils.convertType(
                        rawValue,
                        valueDataType,
                        dateFormat,
                        timeFormat,
                        timestampFormat,
                        fieldName);
            } catch (Exception e) {
                converted = rawValue;
            }
            result.put(key, converted);
        }
        return result;
    }

    private static List<String> splitRespectingQuotes(String s) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            if (c == '"') {
                inQuotes = !inQuotes;
                current.append(c);
            } else if (c == ',' && !inQuotes) {
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            parts.add(current.toString());
        }

        return parts;
    }
}
