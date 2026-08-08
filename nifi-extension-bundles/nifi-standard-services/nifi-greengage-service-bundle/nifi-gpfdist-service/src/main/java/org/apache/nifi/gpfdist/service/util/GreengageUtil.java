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
package org.apache.nifi.gpfdist.service.util;

import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.metadata.GreengageDataType;
import org.postgresql.core.Oid;

import java.math.BigDecimal;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;

public final class GreengageUtil {
    public static final String QUOTE = "\"";
    public static final String IDENTIFIER_DELIMITER = ".";
    public static final Set<GreengageDataType> SUPPORTED_MAX_VALUE_TYPES = EnumSet.of(
            GreengageDataType.MONEY,
            GreengageDataType.DECIMAL,
            GreengageDataType.INTEGER,
            GreengageDataType.SMALLINT,
            GreengageDataType.BIGINT,
            GreengageDataType.REAL,
            GreengageDataType.DOUBLE_PRECISION,
            GreengageDataType.DATE,
            GreengageDataType.TIME,
            GreengageDataType.TIMESTAMP_WITHOUT_TIME_ZONE,
            GreengageDataType.TIMESTAMP_WITH_TIME_ZONE);
    public static final String SUPPORTED_MAX_VALUE_TYPES_DESCRIPTION =
            SUPPORTED_MAX_VALUE_TYPES.stream()
                    .map(e -> e.name().toLowerCase())
                    .collect(Collectors.joining(", "));
    private static final Map<Integer, Integer> TYPE_OID_TO_JDBC_TYPE_MAP = Map.ofEntries(
            Map.entry(Oid.INT2, Types.SMALLINT),
            Map.entry(Oid.INT4, Types.INTEGER),
            Map.entry(Oid.OID, Types.BIGINT),
            Map.entry(Oid.INT8, Types.BIGINT),
            Map.entry(Oid.MONEY, Types.DOUBLE),
            Map.entry(Oid.NUMERIC, Types.NUMERIC),
            Map.entry(Oid.FLOAT4, Types.REAL),
            Map.entry(Oid.FLOAT8, Types.DOUBLE),
            Map.entry(Oid.CHAR, Types.CHAR),
            Map.entry(Oid.BPCHAR, Types.CHAR),
            Map.entry(Oid.VARCHAR, Types.VARCHAR),
            Map.entry(Oid.TEXT, Types.VARCHAR),
            Map.entry(Oid.NAME, Types.VARCHAR),
            Map.entry(Oid.BYTEA, Types.BINARY),
            Map.entry(Oid.BOOL, Types.BIT),
            Map.entry(Oid.BIT, Types.BIT),
            Map.entry(Oid.DATE, Types.DATE),
            Map.entry(Oid.TIME, Types.TIME),
            Map.entry(Oid.TIMETZ, Types.TIME),
            Map.entry(Oid.TIMESTAMP, Types.TIMESTAMP),
            Map.entry(Oid.TIMESTAMPTZ, Types.TIMESTAMP),
            Map.entry(Oid.UUID, Types.OTHER),
            Map.entry(Oid.JSON, Types.OTHER));

    private GreengageUtil() {
    }

    public static String quote(final String identifier) {
        if (identifier != null) {
            return QUOTE + identifier.replace(QUOTE, QUOTE + QUOTE) + QUOTE;
        }
        return null;
    }

    public static String getQualifiedName(final String schemaName, final String tableName) {
        if (schemaName == null || schemaName.isBlank()) {
            return tableName;
        }
        return schemaName + IDENTIFIER_DELIMITER + tableName;
    }

    public static String getFullName(final String schemaName, final String tableName) {
        if (schemaName == null || schemaName.isBlank()) {
            return quote(tableName);
        }
        return quote(schemaName) + IDENTIFIER_DELIMITER + quote(tableName);
    }

    public static Integer getJdbcTypeFromOid(int oid) {
        return Optional.ofNullable(TYPE_OID_TO_JDBC_TYPE_MAP.get(oid))
                .orElseThrow(() -> new IllegalArgumentException(format("JdbcType for oid: %s not found", oid)));
    }

    public static String getLiteralByType(final Map<String, ColumnDataType> maxValueColumnTypes,
                                          final String columnName,
                                          final String value) {
        final GreengageDataType type = maxValueColumnTypes.get(columnName).getType();
        switch (type) {
            case DATE:
            case TIME:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return "'" + value.replace("'", "''") + "'";
            case MONEY:
            case DECIMAL:
            case INTEGER:
            case SMALLINT:
            case BIGINT:
            case REAL:
            case DOUBLE_PRECISION:
                return value;
            default:
                throw new IllegalArgumentException("Column type is not valid for max value tracking: " + type);
        }
    }

    public static String getStateKey(final String tablePrefix, final int workerIndex, final String columnName) {
        return tablePrefix + "_worker_" + workerIndex + "_" + columnName;
    }

    public static int compareByType(final Map<String, ColumnDataType> maxValueColumnTypes,
                                    final String columnName,
                                    final String left,
                                    final String right) {
        final GreengageDataType type = maxValueColumnTypes.get(columnName).getType();
        if (!SUPPORTED_MAX_VALUE_TYPES.contains(type)) {
            throw new IllegalArgumentException("Column type is not valid for max value tracking: " + type);
        }
        switch (type) {
            case MONEY:
            case DECIMAL:
            case INTEGER:
            case SMALLINT:
            case BIGINT:
            case REAL:
            case DOUBLE_PRECISION:
                return new BigDecimal(left).compareTo(new BigDecimal(right));
            case DATE:
                return LocalDate.parse(left).compareTo(LocalDate.parse(right));
            case TIME:
                return LocalTime.parse(left).compareTo(LocalTime.parse(right));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return parseLocalDateTime(left).compareTo(parseLocalDateTime(right));
            case TIMESTAMP_WITH_TIME_ZONE:
                return parseOffsetDateTime(left).compareTo(parseOffsetDateTime(right));
            default:
                throw new IllegalArgumentException("Column type is not valid for max value tracking: " + type);
        }
    }

    private static LocalDateTime parseLocalDateTime(final String value) {
        try {
            return LocalDateTime.parse(value);
        } catch (DateTimeParseException ignored) {
            return LocalDateTime.parse(value.replace(' ', 'T'));
        }
    }

    private static OffsetDateTime parseOffsetDateTime(final String value) {
        final String normalized = normalizeOffsetDateTime(value);
        try {
            return OffsetDateTime.parse(normalized);
        } catch (DateTimeParseException ignored) {
            return OffsetDateTime.parse(normalized + ":00");
        }
    }

    private static String normalizeOffsetDateTime(final String value) {
        final String replaced = value.contains("T") ? value : value.replace(' ', 'T');
        if (replaced.endsWith("Z")) {
            return replaced;
        }
        return replaced.matches(".*[+-]\\d{2}$") ? replaced + ":00" : replaced;
    }
}
