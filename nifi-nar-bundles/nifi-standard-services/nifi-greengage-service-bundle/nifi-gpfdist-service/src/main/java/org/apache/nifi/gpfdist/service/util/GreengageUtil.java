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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;

public final class GreengageUtil {
    public static final String QUOTE = "\"";
    public static final String IDENTIFIER_DELIMITER = ".";
    private final static Map<Integer, Integer> TYPE_OID_TO_JDBC_TYPE_MAP;

    static {
        TYPE_OID_TO_JDBC_TYPE_MAP = new HashMap<>();
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.INT2, Types.SMALLINT);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.INT4, Types.INTEGER);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.OID, Types.BIGINT);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.INT8, Types.BIGINT);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.MONEY, Types.DOUBLE);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.NUMERIC, Types.NUMERIC);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.FLOAT4, Types.REAL);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.FLOAT8, Types.DOUBLE);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.CHAR, Types.CHAR);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.BPCHAR, Types.CHAR);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.VARCHAR, Types.VARCHAR);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.TEXT, Types.VARCHAR);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.NAME, Types.VARCHAR);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.BYTEA, Types.BINARY);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.BOOL, Types.BIT);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.BIT, Types.BIT);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.DATE, Types.DATE);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.TIME, Types.TIME);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.TIMETZ, Types.TIME);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.TIMESTAMP, Types.TIMESTAMP);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.TIMESTAMPTZ, Types.TIMESTAMP);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.UUID, Types.OTHER);
        TYPE_OID_TO_JDBC_TYPE_MAP.put(Oid.JSON, Types.OTHER);
    }

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
