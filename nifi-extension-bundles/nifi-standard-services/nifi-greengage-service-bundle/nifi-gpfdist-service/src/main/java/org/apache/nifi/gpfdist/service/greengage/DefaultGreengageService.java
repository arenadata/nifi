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
package org.apache.nifi.gpfdist.service.greengage;

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.metadata.TableDescription;
import org.apache.nifi.gpfdist.service.GreengageService;
import org.apache.nifi.gpfdist.service.datatype.ArrayDataType;
import org.apache.nifi.gpfdist.service.datatype.BigintDataType;
import org.apache.nifi.gpfdist.service.datatype.BitDataType;
import org.apache.nifi.gpfdist.service.datatype.BooleanDataType;
import org.apache.nifi.gpfdist.service.datatype.ByteaDataType;
import org.apache.nifi.gpfdist.service.datatype.CharDataType;
import org.apache.nifi.gpfdist.service.datatype.DateDataType;
import org.apache.nifi.gpfdist.service.datatype.DecimalDataType;
import org.apache.nifi.gpfdist.service.datatype.DoubleDataType;
import org.apache.nifi.gpfdist.service.datatype.EnumDataType;
import org.apache.nifi.gpfdist.service.datatype.IntegerDataType;
import org.apache.nifi.gpfdist.service.datatype.JsonbDataType;
import org.apache.nifi.gpfdist.service.datatype.MapDataType;
import org.apache.nifi.gpfdist.service.datatype.MoneyDataType;
import org.apache.nifi.gpfdist.service.datatype.RealDataType;
import org.apache.nifi.gpfdist.service.datatype.SmallintDataType;
import org.apache.nifi.gpfdist.service.datatype.TimeDataType;
import org.apache.nifi.gpfdist.service.datatype.TimestampWithTimeZoneDataType;
import org.apache.nifi.gpfdist.service.datatype.TimestampWithoutTimeZoneDataType;
import org.apache.nifi.gpfdist.service.datatype.UuidDataType;
import org.apache.nifi.gpfdist.service.datatype.VarcharDataType;
import org.apache.nifi.gpfdist.service.greengage.model.GreengageColumnDescription;
import org.apache.nifi.gpfdist.service.greengage.model.GreengageTableDescription;
import org.apache.nifi.logging.ComponentLog;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.Math.max;
import static java.util.stream.Collectors.toMap;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.getFullName;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.getJdbcTypeFromOid;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.quote;

public class DefaultGreengageService implements GreengageService {
    private static final int VARCHAR_MAXIMUM_SIZE = 65535;
    private static final int DEFAULT_MONEY_TYPE_SCALE = 2;
    private final DBCPService dbcpService;
    private final ComponentLog logger;
    private Integer moneyTypeScale;

    public DefaultGreengageService(final DBCPService dbcpService, ComponentLog logger) {
        this.dbcpService = dbcpService;
        this.logger = logger;
    }

    @Override
    public DatabaseMetaData getDatabaseMetadata() {
        Connection connection = null;
        try {
            connection = dbcpService.getConnection();
            return connection.getMetaData();
        } catch (Exception e) {
            String errMsg = "Failed to get greengage database metadata";
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        } finally {
            closeConnection(connection);
        }
    }

    @Override
    public TableDescription getTableDescription(final String schemaName, final String tableName) {
        Connection connection = null;
        try {
            connection = dbcpService.getConnection();
            return createTableDescription(connection,
                    schemaName,
                    tableName,
                    false,
                    null,
                    logger);
        } catch (Exception e) {
            String errMsg =
                    "Failed to get table schema: " + String.join("; ", e.getMessage().split("\n"));
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        } finally {
            closeConnection(connection);
        }
    }

    @Override
    public int getSegmentCount() {
        Connection connection = null;
        try {
            connection = dbcpService.getConnection();
            final String sql = "SELECT COUNT(*) " +
                    "FROM gp_segment_configuration " +
                    "WHERE role = 'p' AND content >= 0 AND status = 'u'";
            try (PreparedStatement st = connection.prepareStatement(sql);
                 ResultSet rs = st.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                } else {
                    throw new RuntimeException("Failed to read segment count from gp_segment_configuration");
                }
            }
        } catch (Exception e) {
            String errMsg = "Failed to get Greenplum segment count";
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        } finally {
            closeConnection(connection);
        }
    }

    @Override
    public Map<String, String> getTableColumnsUpperBoundTuples(final String schemaName,
                                                               final String tableName,
                                                               final List<String> columnNames,
                                                               final int globalParallelFactor,
                                                               final int workerIndex) {
        if (columnNames == null || columnNames.isEmpty()) {
            return Map.of();
        }
        final String selectedColumns = columnNames.stream()
                .map(column -> quote(column))
                .collect(Collectors.joining(", "));
        final String orderBy = columnNames.stream()
                .map(column -> quote(column) + " DESC")
                .collect(Collectors.joining(", "));
        final String tablePath = getFullName(schemaName, tableName);
        final String sql = "SELECT " + selectedColumns
                + " FROM " + tablePath
                + " WHERE gp_segment_id % " + globalParallelFactor + " = " + workerIndex
                + " ORDER BY " + orderBy
                + " LIMIT 1";

        Connection connection = null;
        try {
            connection = dbcpService.getConnection();
            try (PreparedStatement st = connection.prepareStatement(sql);
                 ResultSet rs = st.executeQuery()) {
                if (!rs.next()) {
                    return Map.of();
                }
                final Map<String, String> result = new LinkedHashMap<>();
                for (String columnName : columnNames) {
                    final Object value = rs.getObject(columnName);
                    if (value != null) {
                        result.put(columnName, String.valueOf(value));
                    }
                }
                return result;
            }
        } catch (Exception e) {
            String errMsg = "Failed to get upper bound tuple for table " + tableName;
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        } finally {
            closeConnection(connection);
        }
    }

    public GreengageTableDescription createTableDescription(final Connection conn,
                                                            final String schema,
                                                            final String tableName,
                                                            final boolean translateColumnNames,
                                                            final String updateKeys,
                                                            ComponentLog logger) throws SQLException {
        final DatabaseMetaData dmd = conn.getMetaData();
        try (final ResultSet colrs = dmd.getColumns(null, schema, tableName, "%")) {
            final List<GreengageColumnDescription> cols = new ArrayList<>();
            while (colrs.next()) {
                final GreengageColumnDescription col = createColumnDescription(conn, colrs, schema, tableName);
                cols.add(col);
            }
            // If no columns are found, check that the table exists
            if (cols.isEmpty()) {
                try (final ResultSet tblrs = dmd.getTables(null, schema, tableName, null)) {
                    List<String> qualifiedNameSegments = new ArrayList<>();
                    if (schema != null) {
                        qualifiedNameSegments.add(schema);
                    }
                    if (tableName != null) {
                        qualifiedNameSegments.add(tableName);
                    }
                    if (!tblrs.next()) {
                        throw new RuntimeException("Table "
                                + String.join(".", qualifiedNameSegments)
                                + " not found, ensure the Catalog, Schema, and/or Table Names match those in the database exactly");
                    } else {
                        logger.warn("Table "
                                + String.join(".", qualifiedNameSegments)
                                + " found but no columns were found, if this is not expected then check the user permissions for getting table metadata from the database");
                    }
                }
            }

            final Set<String> primaryKeyColumns = new HashSet<>();
            if (updateKeys == null) {
                try (final ResultSet pkrs = dmd.getPrimaryKeys(null, schema, tableName)) {

                    while (pkrs.next()) {
                        final String colName = pkrs.getString("COLUMN_NAME");
                        primaryKeyColumns.add(colName);
                    }
                }
            } else {
                // Parse the Update Keys field and normalize the column names
                for (final String updateKey : updateKeys.split(",")) {
                    primaryKeyColumns.add(normalizeColumnName(updateKey.trim(), translateColumnNames));
                }
            }
            return new GreengageTableDescription(schema, tableName, cols.stream()
                    .collect(toMap(GreengageColumnDescription::getName, Function.identity())));
        }
    }

    private void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warn("Failed to close connection", e);
            }
        }
    }

    public GreengageColumnDescription createColumnDescription(Connection conn,
                                                              ResultSet resultSet,
                                                              String schema,
                                                              String tableName) throws SQLException {
        final ResultSetMetaData md = resultSet.getMetaData();
        List<String> columns = new ArrayList<>();
        for (int i = 1; i < md.getColumnCount() + 1; i++) {
            columns.add(md.getColumnName(i));
        }
        final String defaultValue = resultSet.getString("COLUMN_DEF");
        final String columnName = resultSet.getString("COLUMN_NAME");
        final String jdbcTypeName = resultSet.getString("TYPE_NAME");
        final int jdbcDataType = resultSet.getInt("DATA_TYPE");
        final int colSize = resultSet.getInt("COLUMN_SIZE");
        final Integer decimalDigits = getDecimalDigits(resultSet);
        final String nullableValue = resultSet.getString("IS_NULLABLE");
        boolean isNullable = "YES".equalsIgnoreCase(nullableValue) || nullableValue.isEmpty();
        String autoIncrementValue = "NO";

        if (columns.contains("IS_AUTOINCREMENT")) {
            autoIncrementValue = resultSet.getString("IS_AUTOINCREMENT");
        }

        final boolean isAutoIncrement = "YES".equalsIgnoreCase(autoIncrementValue);
        final boolean required = !isNullable && !isAutoIncrement && defaultValue == null;
        ColumnDataType columnDataType = getColumnDataType(conn,
                schema,
                tableName,
                columnName,
                jdbcTypeName,
                jdbcDataType,
                colSize,
                decimalDigits);
        return new GreengageColumnDescription(columnName, columnDataType, required, isNullable);
    }

    private ColumnDataType getColumnDataType(Connection conn,
                                             String schema,
                                             String tableName,
                                             String columnName,
                                             String jdbcTypeName,
                                             Integer jdbcDataType,
                                             Integer colSize,
                                             Integer decimalDigits) {
        if (jdbcTypeName == null) {
            throw new IllegalArgumentException("jdbcTypeName cannot be null");
        }
        if (conn != null) {
            EnumTypeMetadata enumMeta = getEnumTypeMetadata(conn, schema, tableName, columnName);
            if (enumMeta != null) {
                return new EnumDataType(enumMeta.typeSchema, enumMeta.typeName);
            }
        }
        switch (jdbcTypeName) {
            case "bool":
                return new BooleanDataType();
            case "money":
                return new MoneyDataType(getMoneyScale(conn));
            case "uuid":
                return new UuidDataType();
            case "jsonb":
            case "json":
                return new JsonbDataType();
            case "timestamptz":
                return new TimestampWithTimeZoneDataType(getRequiredDecimalDigits(decimalDigits));
            case "hstore":
                return new MapDataType();
        }
        switch (jdbcDataType) {
            case Types.BIT:
                return new BitDataType(colSize);
            case Types.SMALLINT:
                return new SmallintDataType();
            case Types.INTEGER:
                return new IntegerDataType();
            case Types.BIGINT:
                return new BigintDataType();
            case Types.REAL:
                return new RealDataType();
            case Types.DOUBLE:
                return new DoubleDataType();
            case Types.NUMERIC: {
                int columnSize = getRequiredColumnSize(colSize);
                int precision;
                precision = columnSize + max(-decimalDigits, 0);
                if (columnSize != 0 && precision <= 38) {
                    return new DecimalDataType(precision, max(decimalDigits, 0));
                }
                throw new IllegalArgumentException("Column size of decimal type must be between 0 and 38");
            }
            case Types.CHAR:
                return new CharDataType(getRequiredColumnSize(colSize));
            case Types.VARCHAR:
                int varcharSize = getRequiredColumnSize(colSize);
                if (varcharSize > VARCHAR_MAXIMUM_SIZE) {
                    return new VarcharDataType();
                }
                return new VarcharDataType(varcharSize);
            case Types.BINARY:
                if (jdbcTypeName.equals("bytea")) {
                    return new ByteaDataType();
                }
                throw new IllegalArgumentException("Column type of binary data type is not supported");
            case Types.DATE:
                return new DateDataType();
            case Types.TIME:
                return new TimeDataType(getRequiredColumnSize(colSize));
            case Types.TIMESTAMP:
                return new TimestampWithoutTimeZoneDataType(getRequiredColumnSize(colSize));
            case Types.ARRAY:
                ColumnDataType elementDataType = getArrayElementColumnDataType(conn,
                        schema,
                        tableName,
                        columnName,
                        colSize,
                        decimalDigits);
                return new ArrayDataType(elementDataType);
            default:
                throw new IllegalArgumentException("Unsupported column type: " + jdbcTypeName);
        }
    }

    private EnumTypeMetadata getEnumTypeMetadata(Connection conn, String schema, String table, String column) {
        final String sql =
                "SELECT ns_t.nspname AS type_schema, t.typname AS type_name\n" +
                        "FROM pg_catalog.pg_attribute a\n" +
                        "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid\n" +
                        "JOIN pg_catalog.pg_namespace ns_c ON ns_c.oid = c.relnamespace\n" +
                        "JOIN pg_catalog.pg_type t ON t.oid = a.atttypid\n" +
                        "JOIN pg_catalog.pg_namespace ns_t ON ns_t.oid = t.typnamespace\n" +
                        "WHERE c.relname = ?\n" +
                        "  AND ns_c.nspname = ?\n" +
                        "  AND a.attname = ?\n" +
                        "  AND t.typtype = 'e'\n" +
                        "  AND a.attnum > 0\n" +
                        "  AND NOT a.attisdropped";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, table);
            ps.setString(2, schema == null ? "public" : schema);
            ps.setString(3, column);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new EnumTypeMetadata(rs.getString("type_schema"), rs.getString("type_name"));
                }
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get enum type metadata for column " + column, e);
        }
    }

    private static final class EnumTypeMetadata {
        private final String typeSchema;
        private final String typeName;

        private EnumTypeMetadata(String typeSchema, String typeName) {
            this.typeSchema = typeSchema;
            this.typeName = typeName;
        }
    }

    private ColumnDataType getArrayElementColumnDataType(Connection conn,
                                                         String schema,
                                                         String tableName,
                                                         String columnName,
                                                         Integer colSize,
                                                         Integer decimalDigits) {
        try {
            DataTypeMetadata elementTypeMetadata = getTypeOid(conn, schema, tableName, columnName);
            int jdbcType = getJdbcTypeFromOid(elementTypeMetadata.getOid());
            if (jdbcType == Types.ARRAY) {
                throw new SQLException("Multidimensional array type is not supported");
            }
            return getColumnDataType(conn,
                    schema,
                    tableName,
                    columnName,
                    elementTypeMetadata.getTypeName(),
                    jdbcType,
                    colSize,
                    decimalDigits);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get array element data type", e);
        }
    }

    private DataTypeMetadata getTypeOid(Connection conn, String schema, String table, String columnName) {
        try {
            PreparedStatement statement = conn.prepareStatement("SELECT e.oid, e.typname\n" +
                    "FROM pg_catalog.pg_type t\n" +
                    "         JOIN pg_catalog.pg_type e ON t.typelem = e.oid\n" +
                    "         JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid\n" +
                    "         JOIN (select atttypid\n" +
                    "               from pg_attribute att\n" +
                    "                        join pg_class tbl on tbl.oid = att.attrelid\n" +
                    "                        join pg_namespace ns on tbl.relnamespace = ns.oid\n" +
                    "               where tbl.relname = ? \n" +
                    "                 and ns.nspname = ? \n" +
                    "                 and att.attname = ? ) as tt ON tt.atttypid = t.oid");
            statement.setString(1, table);
            statement.setString(2, schema == null ? "public" : schema);
            statement.setString(3, columnName);
            DataTypeMetadata typeMetadata = null;
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    int oid = rs.getInt("oid");
                    String typeName = rs.getString("typname");
                    typeMetadata = new DataTypeMetadata(oid, typeName);
                }
            }
            if (typeMetadata == null) {
                throw new SQLException("Data type metadata for column " + columnName + " is not found");
            }
            return typeMetadata;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get data type metadata", e);
        }
    }

    private int getRequiredColumnSize(Integer columnSize) {
        return Optional.ofNullable(columnSize).orElseThrow(() -> new IllegalArgumentException("columnSize cannot be null"));
    }

    private int getRequiredDecimalDigits(Integer decimalDigits) {
        return Optional.ofNullable(decimalDigits).orElseThrow(() -> new IllegalArgumentException("decimalDigits cannot be null"));
    }

    private Integer getDecimalDigits(final ResultSet resultSet) throws SQLException {
        final int decimalDigits = resultSet.getInt("DECIMAL_DIGITS");
        if (resultSet.wasNull()) {
            return null;
        } else {
            return decimalDigits;
        }
    }

    private int getMoneyScale(final Connection conn) {
        if (moneyTypeScale != null) {
            return moneyTypeScale;
        }
        if (conn == null) {
            return useDefaultMoneyScale("Connection is null while detecting money scale from greengage");
        }
        final String sql = "WITH v AS (" +
                "SELECT ((0.123456789::money)::numeric)::text AS num_txt) " +
                "SELECT CASE " +
                "WHEN position('.' in num_txt) > 0 THEN length(split_part(num_txt, '.', 2)) " +
                "ELSE 0 END AS money_scale FROM v";
        try (PreparedStatement statement = conn.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            if (rs.next()) {
                moneyTypeScale = rs.getInt("money_scale");
                logger.info("Detected money type scale={} using lc_monetary-aware conversion", moneyTypeScale);
                return moneyTypeScale;
            }
            return useDefaultMoneyScale("Failed to detect money scale from greengage");
        } catch (SQLException e) {
            return useDefaultMoneyScale("Failed to detect money scale from greengage", e);
        }
    }

    private int useDefaultMoneyScale(final String message) {
        logger.warn("{}, using default scale={}", message, DEFAULT_MONEY_TYPE_SCALE);
        moneyTypeScale = DEFAULT_MONEY_TYPE_SCALE;
        return moneyTypeScale;
    }

    private int useDefaultMoneyScale(final String message, final Throwable throwable) {
        logger.warn("{}, using default scale={}", message, DEFAULT_MONEY_TYPE_SCALE, throwable);
        moneyTypeScale = DEFAULT_MONEY_TYPE_SCALE;
        return moneyTypeScale;
    }

    public static String normalizeColumnName(final String colName, final boolean translateColumnNames) {
        return colName == null ? null : (translateColumnNames ? colName.toUpperCase().replace("_", "") : colName);
    }

    private static final class DataTypeMetadata {
        private final int oid;
        private final String typeName;

        private DataTypeMetadata(int oid, String typeName) {
            this.oid = oid;
            this.typeName = typeName;
        }

        public int getOid() {
            return oid;
        }

        public String getTypeName() {
            return typeName;
        }
    }
}
