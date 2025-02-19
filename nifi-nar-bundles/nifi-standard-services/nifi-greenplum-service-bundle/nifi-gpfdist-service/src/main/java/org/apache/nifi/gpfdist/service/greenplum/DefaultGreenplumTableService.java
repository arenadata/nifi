package org.apache.nifi.gpfdist.service.greenplum;

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.metadata.TableDescription;
import org.apache.nifi.gpfdist.service.GreenplumTableService;
import org.apache.nifi.gpfdist.service.datatype.*;
import org.apache.nifi.gpfdist.service.greenplum.model.GreenplumColumnDescription;
import org.apache.nifi.gpfdist.service.greenplum.model.GreenplumTableDescription;
import org.apache.nifi.logging.ComponentLog;

import java.sql.*;
import java.util.*;
import java.util.function.Function;

import static java.lang.Math.max;
import static java.util.stream.Collectors.toMap;
import static org.apache.nifi.gpfdist.service.util.GreenplumUtil.getJdbcTypeFromOid;

public class DefaultGreenplumTableService implements GreenplumTableService {
    private static final int VARCHAR_MAXIMUM_SIZE = 65535;
    private final DBCPService dbcpService;
    private final ComponentLog logger;

    public DefaultGreenplumTableService(final DBCPService dbcpService, ComponentLog logger) {
        this.dbcpService = dbcpService;
        this.logger = logger;
    }

    @Override
    public TableDescription getTableDescription(final String catalog, final String schemaName, final String tableName) {
        Connection connection = null;
        try {
            connection = dbcpService.getConnection();
            return createTableDescription(connection,
                    catalog,
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
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.warn("Failed to close connection", e);
                }
            }
        }
    }

    public GreenplumTableDescription createTableDescription(final Connection conn,
                                                            final String catalog,
                                                            final String schema,
                                                            final String tableName,
                                                            final boolean translateColumnNames,
                                                            final String updateKeys,
                                                            ComponentLog logger) throws SQLException {
        final DatabaseMetaData dmd = conn.getMetaData();
        try (final ResultSet colrs = dmd.getColumns(catalog, schema, tableName, "%")) {
            final List<GreenplumColumnDescription> cols = new ArrayList<>();
            while (colrs.next()) {
                final GreenplumColumnDescription col = createColumnDescription(conn, colrs, schema, tableName);
                cols.add(col);
            }
            // If no columns are found, check that the table exists
            if (cols.isEmpty()) {
                try (final ResultSet tblrs = dmd.getTables(catalog, schema, tableName, null)) {
                    List<String> qualifiedNameSegments = new ArrayList<>();
                    if (catalog != null) {
                        qualifiedNameSegments.add(catalog);
                    }
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
                try (final ResultSet pkrs = dmd.getPrimaryKeys(catalog, schema, tableName)) {

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
            return new GreenplumTableDescription(catalog, schema, tableName, cols.stream()
                    .collect(toMap(GreenplumColumnDescription::getName, Function.identity())));
        }
    }

    public GreenplumColumnDescription createColumnDescription(Connection conn,
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
        return new GreenplumColumnDescription(columnName, columnDataType, required);
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
        switch (jdbcTypeName) {
            case "bool":
                return new BooleanDataType();
            case "money":
                return new MoneyDataType();
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
