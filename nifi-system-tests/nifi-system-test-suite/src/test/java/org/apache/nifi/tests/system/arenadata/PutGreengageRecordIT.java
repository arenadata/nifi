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
import org.apache.nifi.tests.system.arenadata.model.Component;
import org.apache.nifi.tests.system.arenadata.model.DataSourceProperties;
import org.apache.nifi.tests.system.arenadata.service.DockerComposeService;
import org.apache.nifi.tests.system.arenadata.service.JdbcService;
import org.apache.nifi.tests.system.arenadata.service.JdbcServiceFactory;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.function.ThrowingRunnable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.tests.system.arenadata.util.ConfigUtil.getTestConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Feature("Put Greengage Record processor")
public class PutGreengageRecordIT extends NifiSystemContainerizedIT {

    private static final String QUERY_DB_TABLE_PROCESSOR_CLASS_NAME = "org.apache.nifi.processors.standard.QueryDatabaseTable";
    private static final String QUERY_DB_TABLE_PROCESSOR_NAR_ARTIFACT = "nifi-standard-nar";
    private static final String DBCP_SERVICE_CLASS_NAME = "org.apache.nifi.dbcp.DBCPConnectionPool";
    private static final String DBCP_SERVICE_NAR_ARTIFACT = "nifi-dbcp-service-nar";
    private static final String PUT_GP_RECORD_PROCESSOR_CLASS_NAME = "org.apache.nifi.gpfdist.processor.PutGreengageRecord";
    private static final String PUT_GP_RECORD_PROCESSOR_NAR_ARTIFACT = "nifi-greengage-service-nar";
    private static final String ROOT_GROUP_ID = "root";
    private static final String RECORD_READER_SERVICE_CLASS_NAME = "org.apache.nifi.avro.AvroReader";
    private static final String RECORD_READER_SERVICE_NAR_ARTIFACT = "nifi-record-serialization-services-nar";
    private static final String GPFDIST_RECORD_PROCESSING_SERVICE_CLASS_NAME = "org.apache.nifi.gpfdist.service.StandartGpfdistService";
    private static final String GPFDIST_RECORD_PROCESSING_SERVICE_NAR_ARTIFACT = "nifi-greengage-service-nar";
    private static final String RELATION_SUCCESS = "success";
    private static final String RELATION_FAILURE = "failure";
    private static final String PG_TABLE_NAME = "pg_test";
    private static final String GP_TABLE_NAME = "test_table";
    private static final String GP_SCHEMA_NAME = "public";
    private static final String CREATE_EXTENSION_HSTORE_SQL = "CREATE EXTENSION IF NOT EXISTS hstore";
    private static final String CREATE_EXTENSION_UUID_SQL = "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"";
    private static final String CREATE_ENUM_SQL = "CREATE TYPE day AS ENUM ('sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat')";
    private static final String CREATE_TABLE_TEMPLATE_SQL = "CREATE TABLE %s (%s)";
    private static final Map<String, String> TABLE_COLUMNS = new LinkedHashMap<>() {{
        put("id", "INT");
        put("f_bigint", "BIGINT");
        put("f_bit", "BIT");
        put("f_bool", "BOOLEAN");
        put("f_bytea", "BYTEA");
        put("f_char", "CHAR(2)");
        put("code", "VARCHAR(10)");
        put("article", "VARCHAR");
        put("f_date", "DATE");
        put("f_float", "FLOAT8");
        put("f_real", "FLOAT4");
        put("f_jsonb", "JSONB");
        put("f_numeric", "DECIMAL(10, 5)");
        put("f_double", "DOUBLE PRECISION");
        put("f_tinyint", "SMALLINT");
        put("f_smallint", "SMALLINT");
        put("f_time", "TIME");
        put("f_timestampz", "TIMESTAMPTZ");
        put("f_timestamp", "TIMESTAMP");
        put("f_uuid", "UUID");
        put("f_text_array", "TEXT[]");
        put("f_hstore", "HSTORE");
    }};
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
            "from generate_series(1, 100) s(i)";

    private static JdbcService adbService;
    private static JdbcService postgresService;
    private ProcessorEntity queryDbTableProcessor;
    private ProcessorEntity putGpRecordProcessor;

    @BeforeAll
    public static void setup() {
        DockerComposeService composeService = new DockerComposeService(List.of(Component.values()));
        composeService.init();
        JdbcServiceFactory jdbcServiceFactory = new JdbcServiceFactory();
        adbService = jdbcServiceFactory.adbService();
        postgresService = jdbcServiceFactory.postgresService();
    }

    @AfterEach
    public void cleanupTables() {
        adbService.exec(String.format("DROP TABLE IF EXISTS %s", GP_TABLE_NAME));
        postgresService.exec(String.format("DROP TABLE IF EXISTS %s", PG_TABLE_NAME));
    }

    @Test
    @SneakyThrows
    public void testWriteRecordsToAdbWithSupportedDataTypes() {
        String insertQuery = String.format("INSERT INTO %s SELECT * FROM (%s) gen", PG_TABLE_NAME, GENERATE_DATASET_SQL);
        initDataset(TABLE_COLUMNS, insertQuery);
        configureNifiFlow(TABLE_COLUMNS);
        assertWithPooling(() -> assertEquals(100, adbService.queryCountOfRowsInTable(GP_TABLE_NAME)));
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
        assertWithPooling(() -> assertEquals(3, adbService.queryCountOfRowsInTable(GP_TABLE_NAME)));
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
        assertWithPooling(() -> assertEquals(3, adbService.queryCountOfRowsInTable(GP_TABLE_NAME)));
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
        assertErrorMessage(putGpRecordProcessor, "is out of range for type integer");
        assertWithPooling(() -> assertEquals(0, adbService.queryCountOfRowsInTable(GP_TABLE_NAME)));
    }

    @Test
    @SneakyThrows
    public void testUnsupportedTypeNegative() {
        Map<String, String> fieldMap = new LinkedHashMap<>();
        fieldMap.put("f_inet", "INET");
        String insertQuery = String.format("INSERT INTO %s VALUES ('127.0.0.1'), ('192.168.100.128/25'), ('0:0:0:0:0:0:0:1')", PG_TABLE_NAME);
        initDataset(fieldMap, insertQuery);
        configureNifiFlow(fieldMap);
        assertErrorMessage(putGpRecordProcessor, "Unsupported column type: inet");
        assertEquals(0, adbService.queryCountOfRowsInTable(GP_TABLE_NAME));
    }

    @Test
    @Disabled("Bug https://tracker.yandex.ru/ADS-2379")
    @SneakyThrows
    public void testUnsupportedEnumTypeNegative() {
        adbService.exec(CREATE_ENUM_SQL);
        postgresService.exec(CREATE_ENUM_SQL);
        Map<String, String> fieldMap = new LinkedHashMap<>();
        fieldMap.put("f_enum", "DAY");
        String insertQuery = String.format("INSERT INTO %s VALUES ('fri'::day), ('sat'::day), ('sun'::day)", PG_TABLE_NAME);
        initDataset(fieldMap, insertQuery);
        configureNifiFlow(fieldMap);
        assertErrorMessage(putGpRecordProcessor, "Unsupported column type: day");
        assertEquals(0, adbService.queryCountOfRowsInTable(GP_TABLE_NAME));
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
        assertErrorMessage(putGpRecordProcessor, "Schema does not match target column count");
        assertWithPooling(() -> assertEquals(0, adbService.queryCountOfRowsInTable(GP_TABLE_NAME)));
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
        adbService.exec(String.format(CREATE_TABLE_TEMPLATE_SQL, GP_TABLE_NAME, targetFields));
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
        ControllerServiceEntity gpDbcpService = configureDbcpService(getTestConfig().getAdb());
        ControllerServiceEntity gpfdistRecordProcessingService = configureGpfdistRecordProcessingService(gpDbcpService);
        putGpRecordProcessor = configurePutGpRecordProcessor(gpfdistRecordProcessingService, recordReaderService, targetFieldMap);
        ConnectionEntity connectionPgToGp = getClientUtil().createConnection(queryDbTableProcessor, putGpRecordProcessor, RELATION_SUCCESS);
        getClientUtil().waitForValidProcessor(queryDbTableProcessor.getId());
        getClientUtil().waitForValidProcessor(putGpRecordProcessor.getId());
        getClientUtil().startProcessor(queryDbTableProcessor);
        getClientUtil().startProcessor(putGpRecordProcessor);
        getClientUtil().waitForRunningProcessor(queryDbTableProcessor.getId());
        getClientUtil().waitForRunningProcessor(putGpRecordProcessor.getId());
        waitForQueueCount(connectionPgToGp.getId(), 1);
    }

    @SneakyThrows
    @Step("Configure DBCP service")
    private ControllerServiceEntity configureDbcpService(DataSourceProperties dsProperties) {
        ControllerServiceEntity dbcpService = getClientUtil().createControllerService(DBCP_SERVICE_CLASS_NAME,
                ROOT_GROUP_ID, NIFI_GROUP_ID, DBCP_SERVICE_NAR_ARTIFACT, getNiFiVersion());
        Map<String, String> dbcpServiceProperties = new HashMap<>();
        dbcpServiceProperties.put("Database Connection URL", dsProperties.getContainerAddress());
        dbcpServiceProperties.put("Database Driver Class Name", dsProperties.getDriverName());
        dbcpServiceProperties.put("database-driver-locations", dsProperties.getDriverLocation());
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
    private ControllerServiceEntity configureGpfdistRecordProcessingService(ControllerServiceEntity gpDbcpService) {
        ControllerServiceEntity gpfdistRecordProcessingService =
                getClientUtil().createControllerService(GPFDIST_RECORD_PROCESSING_SERVICE_CLASS_NAME,
                        ROOT_GROUP_ID, NIFI_GROUP_ID, GPFDIST_RECORD_PROCESSING_SERVICE_NAR_ARTIFACT, getNiFiVersion());
        Map<String, String> gpfdistRecordProcessingServiceProperties = new HashMap<>();
        gpfdistRecordProcessingServiceProperties.put("put-greengage-record-dcbp-service", gpDbcpService.getId());
        gpfdistRecordProcessingServiceProperties.put("Listening Port", getTestConfig().getGpfdistPort());
        gpfdistRecordProcessingServiceProperties.put("Hostname", getTestConfig().getDockerHostIp());
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
        queryDbTableProperties.put("db-fetch-db-type", "PostgreSQL");
        queryDbTableProperties.put("Table Name", PG_TABLE_NAME);
        queryDbTableProperties.put("Columns to Return", getFieldNamesString(fieldMap));
        getClientUtil().updateProcessorProperties(queryDbTableProcessor, queryDbTableProperties);
        queryDbTableProcessor = getClientUtil().setAutoTerminatedRelationships(queryDbTableProcessor, RELATION_FAILURE);
        return queryDbTableProcessor;
    }

    @SneakyThrows
    @Step("Configure put Greengage record processor")
    private ProcessorEntity configurePutGpRecordProcessor(ControllerServiceEntity gpfdistRecordProcessingService,
                                                          ControllerServiceEntity recordReaderService,
                                                          Map<String, String> fieldMap) {
        ProcessorEntity putGpRecordProcessor = getClientUtil().createProcessor(PUT_GP_RECORD_PROCESSOR_CLASS_NAME,
                NIFI_GROUP_ID, PUT_GP_RECORD_PROCESSOR_NAR_ARTIFACT, getNiFiVersion());
        Map<String, String> putGpRecordProperties = new HashMap<>();
        putGpRecordProperties.put("gpfdist-record-processing-service", gpfdistRecordProcessingService.getId());
        putGpRecordProperties.put("put-greengage-record-record-reader", recordReaderService.getId());
        putGpRecordProperties.put("put-greengage-record-table-name", GP_TABLE_NAME);
        putGpRecordProperties.put("put-greengage-record-schema-name", GP_SCHEMA_NAME);
        putGpRecordProperties.put("put-greengage-table-columns", getFieldNamesString(fieldMap));
        getClientUtil().updateProcessorProperties(putGpRecordProcessor, putGpRecordProperties);
        putGpRecordProcessor = getClientUtil().setAutoTerminatedRelationships(putGpRecordProcessor, Set.of(RELATION_SUCCESS, RELATION_FAILURE));
        return putGpRecordProcessor;
    }

    @SneakyThrows
    private void enableControllerServiceAndWait(ControllerServiceEntity controllerServiceEntity) {
        getClientUtil().enableControllerService(controllerServiceEntity);
        getClientUtil().waitForControllerServicesEnabled(controllerServiceEntity.getParentGroupId(), controllerServiceEntity.getId());
    }

    @Step("Assert with polling")
    private void assertWithPooling(ThrowingRunnable assertion) {
        Awaitility.waitAtMost(Duration.ofSeconds(getTestConfig().getGeneralTimeout()))
                .pollInterval(Duration.ofSeconds(getTestConfig().getPollInterval()))
                .untilAsserted(assertion::run);
    }

    @Step("Assert error message")
    private void assertErrorMessage(ProcessorEntity processor, String expected) {
        assertWithPooling(() -> assertTrue(getNifiClient().getProcessorClient().getProcessor(processor.getId())
                .getBulletins().stream().filter(b -> b.getBulletin().getLevel().equals("ERROR")).findFirst().orElseThrow()
                .getBulletin().getMessage().contains(expected)));
    }

    private String getFieldsString(Map<String, String> fieldMap) {
        return fieldMap.entrySet().stream()
                .map(entry -> String.format("%s %s", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(", "));
    }

    private String getFieldNamesString(Map<String, String> fieldMap) {
        return fieldMap.entrySet().stream()
                .map(Map.Entry::getKey)
                .collect(Collectors.joining(", "));
    }

}
