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

import org.postgresql.core.Oid;

import java.sql.Types;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;

public final class GreengageUtil {
    public static final String QUOTE = "\"";
    public static final String IDENTIFIER_DELIMITER = ".";
    private final static Map<Integer, Integer> TYPE_OID_TO_JDBC_TYPE_MAP = Map.ofEntries(
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
            return QUOTE + identifier + QUOTE;
        }
        return null;
    }

    public static String getFullName(final String schemaName, final String tableName) {
        if (schemaName == null) {
            return quote(tableName);
        }
        return quote(schemaName) + IDENTIFIER_DELIMITER + quote(tableName);
    }

    public static Integer getJdbcTypeFromOid(int oid) {
        return Optional.ofNullable(TYPE_OID_TO_JDBC_TYPE_MAP.get(oid))
                .orElseThrow(() -> new IllegalArgumentException(format("JdbcType for oid: %s not found", oid)));
    }
}
