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
package org.apache.nifi.tests.system.arenadata;

import io.qameta.allure.Feature;
import io.qameta.allure.Step;
import lombok.SneakyThrows;
import org.apache.nifi.tests.system.arenadata.model.DataSourceProperties;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.tests.system.arenadata.util.ConfigUtil.getTestConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(AllureExtension.class)
@Feature("Put Greengage Record processor")
public class PutGreengageRecordIT extends NifiSystemContainerizedIT {

    private static final String QUERY_DB_TABLE_PROCESSOR_CLASS_NAME = "org.apache.nifi.processors.standard.QueryDatabaseTable";
    private static final String QUERY_DB_TABLE_PROCESSOR_NAR_ARTIFACT = "nifi-standard-nar";
    private static final String DBCP_SERVICE_CLASS_NAME = "org.apache.nifi.dbcp.DBCPConnectionPool";
    private static final String DBCP_SERVICE_NAR_ARTIFACT = "nifi-dbcp-service-nar";
    private static final String PUT_GG_RECORD_PROCESSOR_CLASS_NAME = "org.apache.nifi.gpfdist.processor.PutGreengageRecord";
    private static final String PUT_GG_RECORD_PROCESSOR_NAR_ARTIFACT = "nifi-greengage-service-nar";
    private static final String ROOT_GROUP_ID = "root";
    private static final String RECORD_READER_SERVICE_CLASS_NAME = "org.apache.nifi.avro.AvroReader";
    private static final String RECORD_READER_SERVICE_NAR_ARTIFACT = "nifi-record-serialization-services-nar";
    private static final String GPFDIST_RECORD_PROCESSING_SERVICE_CLASS_NAME = "org.apache.nifi.gpfdist.service.StandardGpfdistService";
    private static final String GPFDIST_RECORD_PROCESSING_SERVICE_NAR_ARTIFACT = "nifi-greengage-service-nar";
    private static final String UPDATE_ATTRIBUTE_PROCESSOR_CLASS_NAME = "org.apache.nifi.processors.attributes.UpdateAttribute";
    private static final String UPDATE_ATTRIBUTE_PROCESSOR_NAR_ARTIFACT = "nifi-update-attribute-nar";
    private static final String TARGET_SCHEMA_PROPERTY = "target-schema";
    private static final String TARGET_TABLE_PROPERTY = "target-table";
    private static final String COLS_PROPERTY = "cols";
    private static final String RELATION_SUCCESS = "success";
    private static final String RELATION_FAILURE = "failure";
    private static final String PG_TABLE_NAME = "pg_test";
    private static final String GG_TABLE_NAME = "test_table";
    private static final String GG_SCHEMA_NAME = "public";
    private static final String CREATE_EXTENSION_HSTORE_SQL = "CREATE EXTENSION IF NOT EXISTS hstore";
    private static final String CREATE_EXTENSION_UUID_SQL = "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"";
    private static final String CREATE_TABLE_TEMPLATE_SQL = "CREATE TABLE %s (%s)";
    private static final String ID_COLUMN = "id";

    private static final Map<String, String> TABLE_COLUMNS = new LinkedHashMap<>();
    static {
        TABLE_COLUMNS.put(ID_COLUMN, "BIGSERIAL PRIMARY KEY");
        TABLE_COLUMNS.put("f_int", "INT");
        TABLE_COLUMNS.put("f_bigint", "BIGINT");
        TABLE_COLUMNS.put("f_bit", "BIT");
        TABLE_COLUMNS.put("f_bool", "BOOLEAN");
        TABLE_COLUMNS.put("f_bytea", "BYTEA");
        TABLE_COLUMNS.put("f_char", "CHAR(2)");
        TABLE_COLUMNS.put("code", "VARCHAR(10)");
        TABLE_COLUMNS.put("article", "VARCHAR");
        TABLE_COLUMNS.put("f_date", "DATE");
        TABLE_COLUMNS.put("f_float", "FLOAT8");
        TABLE_COLUMNS.put("f_real", "FLOAT4");
        TABLE_COLUMNS.put("f_jsonb", "JSONB");
        TABLE_COLUMNS.put("f_numeric", "DECIMAL(10, 5)");
        TABLE_COLUMNS.put("f_double", "DOUBLE PRECISION");
        TABLE_COLUMNS.put("f_tinyint", "SMALLINT");
        TABLE_COLUMNS.put("f_smallint", "SMALLINT");
        TABLE_COLUMNS.put("f_time", "TIME");
        TABLE_COLUMNS.put("f_timestampz", "TIMESTAMPTZ");
        TABLE_COLUMNS.put("f_timestamp", "TIMESTAMP");
        TABLE_COLUMNS.put("f_uuid", "UUID");
        TABLE_COLUMNS.put("f_text_array", "TEXT[]");
        TABLE_COLUMNS.put("f_hstore", "HSTORE");
    }

    private static final String GENERATE_DATASET_SQL = "select i,\n" +
            "       25000000000 * random(),\n" +
            "       case when random() > 0.5 then 1 else 0 end::bit,\n" +
            "       true,\n" +
            "       (E'\\\\320\\\\170'::bytea),\n" +
            "       'tt',\n" +
            "       left(md5(i::text), 10),\n" +
            "       md5(random()::text),\n" +
            "       CURRENT_DATE,\n" +
            "       (random() * 1.23456)::float4,\n" +
            "       (random() * 7.434)::float8,\n" +
            "       '{\"a\": \"b\"}'::jsonb,\n" +
            "       45.51123::decimal,\n" +
            "       1.03e1,\n" +
            "       15::smallint,\n" +
            "       5000::smallint,\n" +
            "       CURRENT_TIME,\n" +
            "       CURRENT_TIMESTAMP,\n" +
            "       now()::timestamp(12),\n" +
            "       uuid_generate_v4(),\n" +
            "       ARRAY ['val1','val2'],\n" +
            "       '\"paperback\" => \"243\",\n" +
            "       \"publisher\" => \"postgresqltutorial.com\",\n" +
            "       \"language\"  => \"English\",\n" +
            "       \"ISBN-13\"   => \"978-1449370000\",\n" +
            "       \"weight\"    => \"11.2 ounces\"'::hstore\n" +
            "from generate_series(1, 10000) s(i)";

    private ProcessorEntity queryDbTableProcessor;
    private ProcessorEntity putGgRecordProcessor;
    private ProcessorEntity updateAttributeProcessor;
    private ConnectionEntity connectionPgToGg;

    @AfterEach
    public void cleanupTables() {
        adbService.exec(String.format("DROP TABLE IF EXISTS %s", GG_TABLE_NAME));
        postgresService.exec(String.format("DROP TABLE IF EXISTS %s", PG_TABLE_NAME));
    }

    @Test
    @SneakyThrows
    public void testWriteRecordsToAdbWithSupportedDataTypes() {
        Map<String, String> tableColumnsWithoutId = new LinkedHashMap<>(TABLE_COLUMNS);
        tableColumnsWithoutId.remove(ID_COLUMN);
        String insertColumnList = getFieldNamesString(tableColumnsWithoutId);
        String insertQuery = String.format("INSERT INTO %s (%s) %s", PG_TABLE_NAME, insertColumnList, GENERATE_DATASET_SQL);
        initDataset(TABLE_COLUMNS, insertQuery);
        configureNifiFlow(TABLE_COLUMNS);
        assertWithPooling(() -> assertEquals(10000, adbService.queryCountOfRowsInTable(GG_TABLE_NAME)));
    }

    @Test
    @SneakyThrows
    public void testWriteRecordsToAdbForMoneyDataType() {
        // MONEY data type is not supported by QueryDatabaseTable, so DOUBLE is used for initial data set in PG
        Map<String, String> sourceFieldMap = new LinkedHashMap<>();
        sourceFieldMap.put("f_money", "DOUBLE PRECISION");
        Map<String, String> targetFieldMap = new LinkedHashMap<>();
        targetFieldMap.put("f_money", "MONEY");
        String insertQuery = String.format("INSERT INTO %s VALUES (10000), (1500.50), (0.99)", PG_TABLE_NAME);
        initDataset(sourceFieldMap, targetFieldMap, insertQuery);
        configureNifiFlow(sourceFieldMap, targetFieldMap);
        assertWithPooling(() -> assertEquals(3, adbService.queryCountOfRowsInTable(GG_TABLE_NAME)));
    }

    @Test
    @SneakyThrows
    public void testConversionToAnotherDataType() {
        Map<String, String> sourceFieldMap = new LinkedHashMap<>();
        sourceFieldMap.put("f_integer", "INTEGER");
        sourceFieldMap.put("f_bigint", "BIGINT");
        Map<String, String> targetFieldMap = new LinkedHashMap<>();
        targetFieldMap.put("f_bigint", "BIGINT");
        targetFieldMap.put("f_integer", "INTEGER");
        String insertQuery = String.format("INSERT INTO %s VALUES (-2147483648, 2147483647), (0, 0), (2147483647, -2147483648)", PG_TABLE_NAME);
        initDataset(sourceFieldMap, targetFieldMap, insertQuery);
        configureNifiFlow(sourceFieldMap, targetFieldMap);
        assertWithPooling(() -> assertEquals(3, adbService.queryCountOfRowsInTable(GG_TABLE_NAME)));
    }

    @Test
    @SneakyThrows
    public void testConversionToAnotherDataTypeNegative() {
        Map<String, String> sourceFieldMap = new LinkedHashMap<>();
        sourceFieldMap.put("f_bigint", "BIGINT");
        Map<String, String> targetFieldMap = new LinkedHashMap<>();
        targetFieldMap.put("f_integer", "INTEGER");
        String insertQuery = String.format("INSERT INTO %s VALUES (-2147483649), (0), (2147483648)", PG_TABLE_NAME);
        initDataset(sourceFieldMap, targetFieldMap, insertQuery);
        configureNifiFlow(sourceFieldMap, targetFieldMap);
        assertErrorMessage(putGgRecordProcessor, "is out of range for type integer");
        assertWithPooling(() -> assertEquals(0, adbService.queryCountOfRowsInTable(GG_TABLE_NAME)));
    }

    @Test
    @SneakyThrows
    public void testUnsupportedTypeNegative() {
        Map<String, String> fieldMap = new LinkedHashMap<>();
        fieldMap.put("f_inet", "INET");
        String insertQuery = String.format("INSERT INTO %s VALUES ('127.0.0.1'), ('192.168.100.128/25'), ('0:0:0:0:0:0:0:1')", PG_TABLE_NAME);
        initDataset(fieldMap, insertQuery);
        configureNifiFlow(fieldMap);
        assertErrorMessage(putGgRecordProcessor, "Unsupported column type: inet");
        assertEquals(0, adbService.queryCountOfRowsInTable(GG_TABLE_NAME));
    }

    @Test
    @SneakyThrows
    public void testWriteRecordsToAdbWithEnumType() {
        adbService.exec(CREATE_ENUM_SQL);
        postgresService.exec(CREATE_ENUM_SQL);
        Map<String, String> fieldMap = new LinkedHashMap<>();
        fieldMap.put("f_enum", "DAY");
        String insertQuery = String.format("INSERT INTO %s VALUES ('fri'::day), ('sat'::day), ('sun'::day)", PG_TABLE_NAME);
        initDataset(fieldMap, insertQuery);
        configureNifiFlow(fieldMap);
        assertWithPooling(() -> assertEquals(3, adbService.queryCountOfRowsInTable(GG_TABLE_NAME)));
    }

    @Test
    @SneakyThrows
    public void testInconsistentColumnListNegative() {
        Map<String, String> sourceFieldMap = new LinkedHashMap<>();
        sourceFieldMap.put("f_integer", "INTEGER");
        sourceFieldMap.put("f_bit", "BIT");
        Map<String, String> targetFieldMap = new LinkedHashMap<>();
        targetFieldMap.put("f_integer", "INTEGER");
        String insertQuery = String.format("INSERT INTO %s VALUES (100, 0::bit), (0, 1::bit)", PG_TABLE_NAME);
        initDataset(sourceFieldMap, targetFieldMap, insertQuery);
        configureNifiFlow(sourceFieldMap, targetFieldMap);
        assertErrorMessage(putGgRecordProcessor, "Schema does not match target column count");
        assertWithPooling(() -> assertEquals(0, adbService.queryCountOfRowsInTable(GG_TABLE_NAME)));
    }

    @Test
    @SneakyThrows
    public void testIncrementLoadToAdb() {
        Map<String, String> tableColumnsWithoutId = new LinkedHashMap<>(TABLE_COLUMNS);
        tableColumnsWithoutId.remove(ID_COLUMN);
        String insertColumnList = getFieldNamesString(tableColumnsWithoutId);
        String insertQuery = String.format("INSERT INTO %s (%s) %s", PG_TABLE_NAME, insertColumnList, GENERATE_DATASET_SQL);
        initDataset(TABLE_COLUMNS, insertQuery);
        configureNifiFlow(TABLE_COLUMNS);
        final int incr = 10000;
        assertWithPooling(() -> assertEquals(incr, adbService.queryCountOfRowsInTable(GG_TABLE_NAME)));
        for (int i = 1; i < 3; i++) {
            postgresService.exec(insertQuery);
            int total = incr + incr * i;
            assertWithPooling(() -> assertEquals(total, adbService.queryCountOfRowsInTable(GG_TABLE_NAME)));
        }
    }

    @Test
    @SneakyThrows
    public void testIncrementLoadToAdbWithProcessorRestart() {
        Map<String, String> tableColumnsWithoutId = new LinkedHashMap<>(TABLE_COLUMNS);
        tableColumnsWithoutId.remove(ID_COLUMN);
        String insertColumnList = getFieldNamesString(tableColumnsWithoutId);
        String insertQuery = String.format("INSERT INTO %s (%s) %s", PG_TABLE_NAME, insertColumnList, GENERATE_DATASET_SQL);
        initDataset(TABLE_COLUMNS, insertQuery);
        configureNifiFlow(TABLE_COLUMNS);
        assertWithPooling(() -> assertEquals(10000, adbService.queryCountOfRowsInTable(GG_TABLE_NAME)));
        getClientUtil().stopProcessor(putGgRecordProcessor);
        getClientUtil().waitForStoppedProcessor(putGgRecordProcessor.getId());
        postgresService.exec(insertQuery);
        assertWithPooling(() -> assertTrue(getClientUtil().getQueueSize(connectionPgToGg.getId()).getObjectCount() > 0));
        getClientUtil().startProcessor(putGgRecordProcessor);
        getClientUtil().waitForRunningProcessor(putGgRecordProcessor.getId());
        assertWithPooling(() -> assertEquals(20000, adbService.queryCountOfRowsInTable(GG_TABLE_NAME)));
    }

    @Test
    @SneakyThrows
    public void testExpressionLanguageSupport() {
        Map<String, String> tableColumnsWithoutId = new LinkedHashMap<>(TABLE_COLUMNS);
        tableColumnsWithoutId.remove(ID_COLUMN);
        String insertColumnList = getFieldNamesString(tableColumnsWithoutId);
        String insertQuery = String.format("INSERT INTO %s (%s) %s", PG_TABLE_NAME, insertColumnList, GENERATE_DATASET_SQL);
        initDataset(TABLE_COLUMNS, insertQuery);
        configureNifiFlowWithExpressionLanguage(TABLE_COLUMNS, TABLE_COLUMNS);
        assertWithPooling(() -> assertEquals(10000, adbService.queryCountOfRowsInTable(GG_TABLE_NAME)));
    }

    private void initDataset(Map<String, String> fieldMap, String insertQuery) {
        initDataset(fieldMap, fieldMap, insertQuery);
    }

    @Step("Init dataset")
    private void initDataset(Map<String, String> sourceFieldMap, Map<String, String> targetFieldMap, String insertQuery) {
        String sourceFields = getFieldsString(sourceFieldMap);
        String targetFields = getFieldsString(targetFieldMap);
        adbService.exec(CREATE_EXTENSION_HSTORE_SQL);
        adbService.exec(CREATE_EXTENSION_UUID_SQL);
        postgresService.exec(CREATE_EXTENSION_HSTORE_SQL);
        postgresService.exec(CREATE_EXTENSION_UUID_SQL);
        adbService.exec(String.format(CREATE_TABLE_TEMPLATE_SQL, GG_TABLE_NAME, targetFields));
        postgresService.exec(String.format(CREATE_TABLE_TEMPLATE_SQL, PG_TABLE_NAME, sourceFields));
        postgresService.exec(insertQuery);
    }

    @SneakyThrows
    private void configureNifiFlow(Map<String, String> fieldMap) {
        configureNifiFlow(fieldMap, fieldMap);
    }

    @SneakyThrows
    @Step("Configure NiFi flow")
    private void configureNifiFlow(Map<String, String> sourceFieldMap, Map<String, String> targetFieldMap) {
        ControllerServiceEntity pgDbcpService = configureDbcpService(getTestConfig().getPostgres());
        queryDbTableProcessor = configureQueryDbTableProcessor(pgDbcpService, sourceFieldMap);
        ControllerServiceEntity recordReaderService = configureRecordReaderService();
        ControllerServiceEntity ggDbcpService = configureDbcpService(getTestConfig().getAdb());
        ControllerServiceEntity gpfdistRecordProcessingService = configureGpfdistRecordProcessingService(ggDbcpService);
        putGgRecordProcessor = configurePutGgRecordProcessor(gpfdistRecordProcessingService, recordReaderService, targetFieldMap);
        connectionPgToGg = getClientUtil().createConnection(queryDbTableProcessor, putGgRecordProcessor, RELATION_SUCCESS);
        getClientUtil().waitForValidProcessor(queryDbTableProcessor.getId());
        getClientUtil().waitForValidProcessor(putGgRecordProcessor.getId());
        getClientUtil().startProcessor(queryDbTableProcessor);
        getClientUtil().startProcessor(putGgRecordProcessor);
        getClientUtil().waitForRunningProcessor(queryDbTableProcessor.getId());
        getClientUtil().waitForRunningProcessor(putGgRecordProcessor.getId());
        waitForQueueCount(connectionPgToGg.getId(), 1);
    }

    @SneakyThrows
    @Step("Configure NiFi flow with expression language")
    private void configureNifiFlowWithExpressionLanguage(Map<String, String> sourceFieldMap,
                                                         Map<String, String> targetFieldMap) {
        ControllerServiceEntity pgDbcpService = configureDbcpService(getTestConfig().getPostgres());
        queryDbTableProcessor = configureQueryDbTableProcessor(pgDbcpService, sourceFieldMap);
        updateAttributeProcessor = configureUpdateAttributeProcessor(targetFieldMap);
        ControllerServiceEntity recordReaderService = configureRecordReaderService();
        ControllerServiceEntity ggDbcpService = configureDbcpService(getTestConfig().getAdb());
        ControllerServiceEntity gpfdistRecordProcessingService = configureGpfdistRecordProcessingService(ggDbcpService);
        putGgRecordProcessor = configurePutGgRecordProcessorWithExpressionLanguage(gpfdistRecordProcessingService,
                recordReaderService);
        getClientUtil().createConnection(queryDbTableProcessor, updateAttributeProcessor, RELATION_SUCCESS);
        ConnectionEntity connectionUpdateAttributeToGg =
                getClientUtil().createConnection(updateAttributeProcessor, putGgRecordProcessor, RELATION_SUCCESS);
        getClientUtil().waitForValidProcessor(queryDbTableProcessor.getId());
        getClientUtil().waitForValidProcessor(updateAttributeProcessor.getId());
        getClientUtil().waitForValidProcessor(putGgRecordProcessor.getId());
        getClientUtil().startProcessor(queryDbTableProcessor);
        getClientUtil().startProcessor(updateAttributeProcessor);
        getClientUtil().startProcessor(putGgRecordProcessor);
        getClientUtil().waitForRunningProcessor(queryDbTableProcessor.getId());
        getClientUtil().waitForRunningProcessor(updateAttributeProcessor.getId());
        getClientUtil().waitForRunningProcessor(putGgRecordProcessor.getId());
        waitForQueueCount(connectionUpdateAttributeToGg.getId(), 1);
    }

    @SneakyThrows
    @Step("Configure DBCP service")
    private ControllerServiceEntity configureDbcpService(DataSourceProperties dsProperties) {
        ControllerServiceEntity dbcpService = getClientUtil().createControllerService(DBCP_SERVICE_CLASS_NAME,
                ROOT_GROUP_ID, NIFI_GROUP_ID, DBCP_SERVICE_NAR_ARTIFACT, getNiFiVersion());
        Map<String, String> dbcpServiceProperties = new HashMap<>();
        dbcpServiceProperties.put("Database Connection URL", dsProperties.getContainerAddress());
        dbcpServiceProperties.put("Database Driver Class Name", dsProperties.getDriverName());
        dbcpServiceProperties.put("Database Driver Locations", dsProperties.getDriverLocation());
        dbcpServiceProperties.put("Database User", dsProperties.getUsername());
        if (dsProperties.getPassword() != null) {
            dbcpServiceProperties.put("Password", dsProperties.getPassword());
        }
        getClientUtil().updateControllerServiceProperties(dbcpService, dbcpServiceProperties);
        enableControllerServiceAndWait(dbcpService);
        return dbcpService;
    }

    @SneakyThrows
    @Step("Configure record reader service")
    private ControllerServiceEntity configureRecordReaderService() {
        ControllerServiceEntity recordReaderService = getClientUtil().createControllerService(RECORD_READER_SERVICE_CLASS_NAME,
                ROOT_GROUP_ID, NIFI_GROUP_ID, RECORD_READER_SERVICE_NAR_ARTIFACT, getNiFiVersion());
        enableControllerServiceAndWait(recordReaderService);
        return recordReaderService;
    }

    @SneakyThrows
    @Step("Configure gpfdist record processing service")
    private ControllerServiceEntity configureGpfdistRecordProcessingService(ControllerServiceEntity ggDbcpService) {
        ControllerServiceEntity gpfdistRecordProcessingService =
                getClientUtil().createControllerService(GPFDIST_RECORD_PROCESSING_SERVICE_CLASS_NAME,
                        ROOT_GROUP_ID, NIFI_GROUP_ID, GPFDIST_RECORD_PROCESSING_SERVICE_NAR_ARTIFACT, getNiFiVersion());
        Map<String, String> gpfdistRecordProcessingServiceProperties = new HashMap<>();
        gpfdistRecordProcessingServiceProperties.put("put-greengage-record-dcbp-service", ggDbcpService.getId());
        gpfdistRecordProcessingServiceProperties.put("Listening Port", getTestConfig().getGpfdistPort());
        gpfdistRecordProcessingServiceProperties.put("Hostname", getTestConfig().getDockerHostIp());
        gpfdistRecordProcessingServiceProperties.put("Total Nifi Nodes", "3");
        getClientUtil().updateControllerServiceProperties(gpfdistRecordProcessingService, gpfdistRecordProcessingServiceProperties);
        enableControllerServiceAndWait(gpfdistRecordProcessingService);
        return gpfdistRecordProcessingService;
    }

    @SneakyThrows
    @Step("Configure query DB table processor")
    private ProcessorEntity configureQueryDbTableProcessor(ControllerServiceEntity dbcpService, Map<String, String> fieldMap) {
        ProcessorEntity queryDbTableProcessor = getClientUtil().createProcessor(QUERY_DB_TABLE_PROCESSOR_CLASS_NAME,
                NIFI_GROUP_ID, QUERY_DB_TABLE_PROCESSOR_NAR_ARTIFACT, getNiFiVersion());
        Map<String, String> queryDbTableProperties = new HashMap<>();
        queryDbTableProperties.put("Database Connection Pooling Service", dbcpService.getId());
        queryDbTableProperties.put("Database Type", "PostgreSQL");
        queryDbTableProperties.put("Table Name", PG_TABLE_NAME);
        queryDbTableProperties.put("Columns to Return", getFieldNamesString(fieldMap));
        if (fieldMap.containsKey(ID_COLUMN)) {
            queryDbTableProperties.put("Maximum-value Columns", ID_COLUMN);
        }
        getClientUtil().updateProcessorProperties(queryDbTableProcessor, queryDbTableProperties);
        queryDbTableProcessor = getClientUtil().setAutoTerminatedRelationships(queryDbTableProcessor, RELATION_FAILURE);
        return queryDbTableProcessor;
    }

    @SneakyThrows
    @Step("Configure put Greengage record processor")
    private ProcessorEntity configurePutGgRecordProcessor(ControllerServiceEntity gpfdistRecordProcessingService,
                                                          ControllerServiceEntity recordReaderService,
                                                          Map<String, String> fieldMap) {
        ProcessorEntity putGgRecordProcessor = getClientUtil().createProcessor(PUT_GG_RECORD_PROCESSOR_CLASS_NAME,
                NIFI_GROUP_ID, PUT_GG_RECORD_PROCESSOR_NAR_ARTIFACT, getNiFiVersion());
        Map<String, String> putGgRecordProperties = new HashMap<>();
        putGgRecordProperties.put("gpfdist-record-processing-service", gpfdistRecordProcessingService.getId());
        putGgRecordProperties.put("put-greengage-record-record-reader", recordReaderService.getId());
        putGgRecordProperties.put("put-greengage-record-table-name", GG_TABLE_NAME);
        putGgRecordProperties.put("put-greengage-record-schema-name", GG_SCHEMA_NAME);
        putGgRecordProperties.put("put-greengage-table-columns", getFieldNamesString(fieldMap));
        getClientUtil().updateProcessorProperties(putGgRecordProcessor, putGgRecordProperties);
        putGgRecordProcessor = getClientUtil().setAutoTerminatedRelationships(putGgRecordProcessor, Set.of(RELATION_SUCCESS, RELATION_FAILURE));
        return putGgRecordProcessor;
    }

    @SneakyThrows
    @Step("Configure put Greengage record processor with expression language")
    private ProcessorEntity configurePutGgRecordProcessorWithExpressionLanguage(ControllerServiceEntity gpfdistRecordProcessingService,
                                                                                ControllerServiceEntity recordReaderService) {
        ProcessorEntity putGgRecordProcessor = getClientUtil().createProcessor(PUT_GG_RECORD_PROCESSOR_CLASS_NAME,
                NIFI_GROUP_ID, PUT_GG_RECORD_PROCESSOR_NAR_ARTIFACT, getNiFiVersion());
        Map<String, String> putGgRecordProperties = new HashMap<>();
        putGgRecordProperties.put("gpfdist-record-processing-service", gpfdistRecordProcessingService.getId());
        putGgRecordProperties.put("put-greengage-record-record-reader", recordReaderService.getId());
        putGgRecordProperties.put("put-greengage-record-table-name", String.format("${%s}", TARGET_TABLE_PROPERTY));
        putGgRecordProperties.put("put-greengage-record-schema-name", String.format("${%s}", TARGET_SCHEMA_PROPERTY));
        putGgRecordProperties.put("put-greengage-table-columns", String.format("${%s}", COLS_PROPERTY));
        getClientUtil().updateProcessorProperties(putGgRecordProcessor, putGgRecordProperties);
        putGgRecordProcessor = getClientUtil().setAutoTerminatedRelationships(putGgRecordProcessor,
                Set.of(RELATION_SUCCESS, RELATION_FAILURE));
        return putGgRecordProcessor;
    }

    @SneakyThrows
    @Step("Configure update attribute processor")
    private ProcessorEntity configureUpdateAttributeProcessor(Map<String, String> fieldMap) {
        ProcessorEntity updateAttributeProcessor =
                getClientUtil().createProcessor(UPDATE_ATTRIBUTE_PROCESSOR_CLASS_NAME,
                        NIFI_GROUP_ID, UPDATE_ATTRIBUTE_PROCESSOR_NAR_ARTIFACT, getNiFiVersion());
        Map<String, String> updateAttributeProperties = new HashMap<>();
        updateAttributeProperties.put(TARGET_SCHEMA_PROPERTY, GG_SCHEMA_NAME);
        updateAttributeProperties.put(TARGET_TABLE_PROPERTY, GG_TABLE_NAME);
        updateAttributeProperties.put(COLS_PROPERTY, getFieldNamesString(fieldMap));
        getClientUtil().updateProcessorProperties(updateAttributeProcessor, updateAttributeProperties);
        updateAttributeProcessor = getClientUtil().setAutoTerminatedRelationships(updateAttributeProcessor,
                Set.of(RELATION_SUCCESS, RELATION_FAILURE));
        return updateAttributeProcessor;
    }
}
