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

@ExtendWith(AllureExtension.class)
@Feature("Get Greengage Record processor")
public class GetGreengageRecordIT extends NifiSystemContainerizedIT {

    private static final String GET_GG_RECORD_PROCESSOR_CLASS_NAME = "org.apache.nifi.gpfdist.processor.GetGreengageRecord";
    private static final String GET_GG_RECORD_PROCESSOR_NAR_ARTIFACT = "nifi-greengage-service-nar";
    private static final String PUT_DATABASE_RECORD_PROCESSOR_CLASS_NAME = "org.apache.nifi.processors.standard.PutDatabaseRecord";
    private static final String STANDARD_NAR_ARTIFACT = "nifi-standard-nar";
    private static final String DBCP_SERVICE_CLASS_NAME = "org.apache.nifi.dbcp.DBCPConnectionPool";
    private static final String DBCP_SERVICE_NAR_ARTIFACT = "nifi-dbcp-service-nar";
    private static final String ROOT_GROUP_ID = "root";
    private static final String AVRO_RECORD_SET_WRITER_CLASS_NAME = "org.apache.nifi.avro.AvroRecordSetWriter";
    private static final String AVRO_READER_CLASS_NAME = "org.apache.nifi.avro.AvroReader";
    private static final String RECORD_SERIALIZATION_NAR_ARTIFACT = "nifi-record-serialization-services-nar";
    private static final String GPFDIST_RECORD_PROCESSING_SERVICE_CLASS_NAME = "org.apache.nifi.gpfdist.service.StandartGpfdistService";
    private static final String GPFDIST_RECORD_PROCESSING_SERVICE_NAR_ARTIFACT = "nifi-greengage-service-nar";
    private static final String RELATION_SUCCESS = "success";
    private static final String RELATION_FAILURE = "failure";
    private static final String RELATION_RETRY = "retry";
    private static final String GG_TABLE_NAME = "test_table";
    private static final String GG_SCHEMA_NAME = "public";
    private static final String PG_TABLE_NAME = "pg_test";
    private static final String CREATE_EXTENSION_HSTORE_SQL = "CREATE EXTENSION IF NOT EXISTS hstore";
    private static final String CREATE_EXTENSION_UUID_SQL = "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"";
    private static final String CREATE_TABLE_TEMPLATE_SQL = "CREATE TABLE %s (%s)";
    private static final String ID_COLUMN = "id";
    private static final Map<String, String> TABLE_COLUMNS = new LinkedHashMap<>() {{
        put(ID_COLUMN, "BIGSERIAL PRIMARY KEY");
        put("f_int", "INT");
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
        put("f_enum", "DAY");
    }};
    /**
     * PG target columns derived from ADB.
     * Types that GetGreengageRecord converts are mapped to PG-compatible types:
     * MONEY → DOUBLE PRECISION (converted to DECIMAL), DAY (ENUM) → VARCHAR (converted to STRING).
     * <p>
     * Note: f_text_array (TEXT[]) and f_hstore (HSTORE) are excluded from PG target.
     * GetGreengageRecord converts TEXT[] to ARRAY and HSTORE to MAP NiFi record types.
     */
    private static final Map<String, String> PG_TABLE_COLUMNS;
    static {
        PG_TABLE_COLUMNS = new LinkedHashMap<>(TABLE_COLUMNS);
        PG_TABLE_COLUMNS.remove("f_text_array");
        PG_TABLE_COLUMNS.remove("f_hstore");
        PG_TABLE_COLUMNS.put("f_bit", "BOOLEAN");
        PG_TABLE_COLUMNS.put("f_enum", "VARCHAR");
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
            "       ARRAY['foo', 'bar']::text[],\n" +
            "       '\"a\"=>\"1\", \"b\"=>\"2\"'::hstore,\n" +
            "       (ARRAY['sun','mon','tue','wed','thu','fri','sat'])[1 + (i % 7)]::day\n" +
            "from generate_series(1, 100000) s(i)";

    private ProcessorEntity getGgRecordProcessor;
    private ProcessorEntity putDbRecordProcessor;

    @AfterEach
    public void cleanupTables() {
        adbService.exec(String.format("DROP TABLE IF EXISTS %s", GG_TABLE_NAME));
        postgresService.exec(String.format("DROP TABLE IF EXISTS %s", PG_TABLE_NAME));
    }

    @Test
    @SneakyThrows
    public void testReadRecordsFromAdbWithSupportedDataTypes() {
        adbService.exec(CREATE_ENUM_SQL);
        Map<String, String> tableColumnsWithoutId = new LinkedHashMap<>(TABLE_COLUMNS);
        tableColumnsWithoutId.remove(ID_COLUMN);
        String insertColumnList = getFieldNamesString(tableColumnsWithoutId);
        String insertQuery = String.format("INSERT INTO %s (%s) %s", GG_TABLE_NAME, insertColumnList, GENERATE_DATASET_SQL);
        initDataset(TABLE_COLUMNS, PG_TABLE_COLUMNS, insertQuery);
        configureNifiFlow(TABLE_COLUMNS);
        assertWithPooling(() -> assertEquals(100000, postgresService.queryCountOfRowsInTable(PG_TABLE_NAME)));
    }

    @Test
    @SneakyThrows
    public void testReadRecordsFromAdbForMoneyDataType() {
        Map<String, String> sourceFieldMap = new LinkedHashMap<>();
        sourceFieldMap.put("f_money", "MONEY");
        Map<String, String> targetFieldMap = new LinkedHashMap<>();
        targetFieldMap.put("f_money", "DOUBLE PRECISION");
        String insertQuery = String.format("INSERT INTO %s VALUES (10000::money), (1500.50::money), (0.99::money)", GG_TABLE_NAME);
        initDataset(sourceFieldMap, targetFieldMap, insertQuery);
        configureNifiFlow(sourceFieldMap);
        assertWithPooling(() -> assertEquals(3, postgresService.queryCountOfRowsInTable(PG_TABLE_NAME)));
    }

    @Test
    @SneakyThrows
    public void testConversionToAnotherDataType() {
        Map<String, String> sourceFieldMap = new LinkedHashMap<>();
        sourceFieldMap.put("f_integer", "INTEGER");
        sourceFieldMap.put("f_bigint", "BIGINT");
        Map<String, String> targetFieldMap = new LinkedHashMap<>();
        targetFieldMap.put("f_integer", "BIGINT");
        targetFieldMap.put("f_bigint", "INTEGER");
        String insertQuery = String.format("INSERT INTO %s VALUES (-2147483648, 2147483647), (0, 0), (2147483647, -2147483648)", GG_TABLE_NAME);
        initDataset(sourceFieldMap, targetFieldMap, insertQuery);
        configureNifiFlow(sourceFieldMap);
        assertWithPooling(() -> assertEquals(3, postgresService.queryCountOfRowsInTable(PG_TABLE_NAME)));
    }

    @Test
    @SneakyThrows
    public void testConversionToAnotherDataTypeNegative() {
        Map<String, String> sourceFieldMap = new LinkedHashMap<>();
        sourceFieldMap.put("f_bigint", "BIGINT");
        Map<String, String> targetFieldMap = new LinkedHashMap<>();
        targetFieldMap.put("f_bigint", "INTEGER");
        String insertQuery = String.format("INSERT INTO %s VALUES (-2147483649), (0), (2147483648)", GG_TABLE_NAME);
        initDataset(sourceFieldMap, targetFieldMap, insertQuery);
        configureNifiFlow(sourceFieldMap);
        assertErrorMessage(putDbRecordProcessor, "integer out of range");
        assertWithPooling(() -> assertEquals(0, postgresService.queryCountOfRowsInTable(PG_TABLE_NAME)));
    }

    @Test
    @SneakyThrows
    public void testUnsupportedTypeNegative() {
        Map<String, String> fieldMap = new LinkedHashMap<>();
        fieldMap.put("f_inet", "INET");
        String insertQuery = String.format("INSERT INTO %s VALUES ('127.0.0.1'), ('192.168.100.128/25'), ('0:0:0:0:0:0:0:1')", GG_TABLE_NAME);
        initDataset(fieldMap, insertQuery);
        configureNifiFlow(fieldMap);
        assertErrorMessage(getGgRecordProcessor, "Unsupported column type: inet");
        assertEquals(0, postgresService.queryCountOfRowsInTable(PG_TABLE_NAME));
    }

    @Test
    @SneakyThrows
    public void testInconsistentColumnListNegative() {
        Map<String, String> sourceFieldMap = new LinkedHashMap<>();
        sourceFieldMap.put("f_integer", "INTEGER");
        sourceFieldMap.put("f_bit", "BIT");
        Map<String, String> targetFieldMap = new LinkedHashMap<>();
        targetFieldMap.put("f_integer", "INTEGER");
        String insertQuery = String.format("INSERT INTO %s VALUES (100), (0)", GG_TABLE_NAME);
        initDataset(targetFieldMap, targetFieldMap, insertQuery);
        configureNifiFlow(sourceFieldMap);
        assertErrorMessage(getGgRecordProcessor, "Column f_bit not found in table");
        assertWithPooling(() -> assertEquals(0, postgresService.queryCountOfRowsInTable(PG_TABLE_NAME)));
    }

    @Test
    @SneakyThrows
    public void testIncrementLoadFromAdb() {
        adbService.exec(CREATE_ENUM_SQL);
        Map<String, String> tableColumnsWithoutId = new LinkedHashMap<>(TABLE_COLUMNS);
        tableColumnsWithoutId.remove(ID_COLUMN);
        String insertColumnList = getFieldNamesString(tableColumnsWithoutId);
        String insertQuery = String.format("INSERT INTO %s (%s) %s", GG_TABLE_NAME, insertColumnList, GENERATE_DATASET_SQL);
        initDataset(TABLE_COLUMNS, PG_TABLE_COLUMNS, insertQuery);
        configureNifiFlow(TABLE_COLUMNS);
        final int incr = 100000;
        assertWithPooling(() -> assertEquals(incr, postgresService.queryCountOfRowsInTable(PG_TABLE_NAME)));
        for (int i = 1; i < 10; i++) {
            adbService.exec(insertQuery);
            int total = incr + incr * i;
            assertWithPooling(() -> assertEquals(total, postgresService.queryCountOfRowsInTable(PG_TABLE_NAME)));
        }
    }

    @Test
    @SneakyThrows
    public void testIncrementLoadFromAdbWithProcessorRestart() {
        adbService.exec(CREATE_ENUM_SQL);
        Map<String, String> tableColumnsWithoutId = new LinkedHashMap<>(TABLE_COLUMNS);
        tableColumnsWithoutId.remove(ID_COLUMN);
        String insertColumnList = getFieldNamesString(tableColumnsWithoutId);
        String insertQuery = String.format("INSERT INTO %s (%s) %s", GG_TABLE_NAME, insertColumnList, GENERATE_DATASET_SQL);
        initDataset(TABLE_COLUMNS, PG_TABLE_COLUMNS, insertQuery);
        configureNifiFlow(TABLE_COLUMNS);
        assertWithPooling(() -> assertEquals(100000, postgresService.queryCountOfRowsInTable(PG_TABLE_NAME)));
        getClientUtil().stopProcessor(getGgRecordProcessor);
        getClientUtil().waitForStoppedProcessor(getGgRecordProcessor.getId());
        adbService.exec(insertQuery);
        getClientUtil().startProcessor(getGgRecordProcessor);
        getClientUtil().waitForRunningProcessor(getGgRecordProcessor.getId());
        assertWithPooling(() -> assertEquals(200000, postgresService.queryCountOfRowsInTable(PG_TABLE_NAME)));
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
        adbService.exec(String.format(CREATE_TABLE_TEMPLATE_SQL, GG_TABLE_NAME, sourceFields));
        postgresService.exec(String.format(CREATE_TABLE_TEMPLATE_SQL, PG_TABLE_NAME, targetFields));
        adbService.exec(insertQuery);
    }

    /**
     * Configures the NiFi flow:
     * GetGreengageRecord → Avro → PutDbRecord
     * PutDatabaseRecord reads Avro records, builds SQL by PG metadata and converts NiFi records to JDBC types.
     */
    @SneakyThrows
    @Step("Configure NiFi flow")
    private void configureNifiFlow(Map<String, String> sourceFieldMap) {
        ControllerServiceEntity ggDbcpService = configureDbcpService(getTestConfig().getAdb());
        ControllerServiceEntity gpfdistRecordProcessingService = configureGpfdistRecordProcessingService(ggDbcpService);
        ControllerServiceEntity avroWriterService = configureAvroRecordSetWriterService();
        getGgRecordProcessor = configureGetGgRecordProcessor(gpfdistRecordProcessingService, avroWriterService, sourceFieldMap);
        ControllerServiceEntity avroReaderService = configureAvroReaderService();
        ControllerServiceEntity pgDbcpService = configureDbcpService(getTestConfig().getPostgres());
        putDbRecordProcessor = configurePutDatabaseRecordProcessor(pgDbcpService, avroReaderService);
        getClientUtil().createConnection(getGgRecordProcessor, putDbRecordProcessor, RELATION_SUCCESS);
        getClientUtil().waitForValidProcessor(getGgRecordProcessor.getId());
        getClientUtil().waitForValidProcessor(putDbRecordProcessor.getId());
        getClientUtil().startProcessor(getGgRecordProcessor);
        getClientUtil().startProcessor(putDbRecordProcessor);
        getClientUtil().waitForRunningProcessor(getGgRecordProcessor.getId());
        getClientUtil().waitForRunningProcessor(putDbRecordProcessor.getId());
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
    @Step("Configure Avro record set writer service")
    private ControllerServiceEntity configureAvroRecordSetWriterService() {
        ControllerServiceEntity service = getClientUtil().createControllerService(AVRO_RECORD_SET_WRITER_CLASS_NAME,
                ROOT_GROUP_ID, NIFI_GROUP_ID, RECORD_SERIALIZATION_NAR_ARTIFACT, getNiFiVersion());
        enableControllerServiceAndWait(service);
        return service;
    }

    @SneakyThrows
    @Step("Configure Avro reader service")
    private ControllerServiceEntity configureAvroReaderService() {
        ControllerServiceEntity service = getClientUtil().createControllerService(AVRO_READER_CLASS_NAME,
                ROOT_GROUP_ID, NIFI_GROUP_ID, RECORD_SERIALIZATION_NAR_ARTIFACT, getNiFiVersion());
        enableControllerServiceAndWait(service);
        return service;
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
        getClientUtil().updateControllerServiceProperties(gpfdistRecordProcessingService, gpfdistRecordProcessingServiceProperties);
        enableControllerServiceAndWait(gpfdistRecordProcessingService);
        return gpfdistRecordProcessingService;
    }

    @SneakyThrows
    @Step("Configure get Greengage record processor")
    private ProcessorEntity configureGetGgRecordProcessor(ControllerServiceEntity gpfdistRecordProcessingService,
                                                          ControllerServiceEntity recordWriterService,
                                                          Map<String, String> fieldMap) {
        ProcessorEntity getGgRecordProcessor = getClientUtil().createProcessor(GET_GG_RECORD_PROCESSOR_CLASS_NAME,
                NIFI_GROUP_ID, GET_GG_RECORD_PROCESSOR_NAR_ARTIFACT, getNiFiVersion());
        Map<String, String> getGgRecordProperties = new HashMap<>();
        getGgRecordProperties.put("gpfdist-record-processing-service", gpfdistRecordProcessingService.getId());
        getGgRecordProperties.put("record-writer", recordWriterService.getId());
        getGgRecordProperties.put("get-greengage-record-table-name", GG_TABLE_NAME);
        getGgRecordProperties.put("get-greengage-record-schema-name", GG_SCHEMA_NAME);
        getGgRecordProperties.put("get-greengage-table-columns", getFieldNamesString(fieldMap));
        if (fieldMap.containsKey(ID_COLUMN)) {
            getGgRecordProperties.put("Maximum-value Columns", ID_COLUMN);
        }
        getClientUtil().updateProcessorProperties(getGgRecordProcessor, getGgRecordProperties);
        getGgRecordProcessor = getClientUtil().setAutoTerminatedRelationships(getGgRecordProcessor, RELATION_FAILURE);
        return getGgRecordProcessor;
    }

    /**
     * Configures PutDatabaseRecord processor.
     * Avro records are inserted into PostgreSQL with type conversion.
     */
    @SneakyThrows
    @Step("Configure PutDatabaseRecord processor")
    private ProcessorEntity configurePutDatabaseRecordProcessor(ControllerServiceEntity pgDbcpService,
                                                                ControllerServiceEntity avroReaderService) {
        ProcessorEntity processor = getClientUtil().createProcessor(PUT_DATABASE_RECORD_PROCESSOR_CLASS_NAME,
                NIFI_GROUP_ID, STANDARD_NAR_ARTIFACT, getNiFiVersion());
        Map<String, String> properties = new HashMap<>();
        properties.put("put-db-record-dcbp-service", pgDbcpService.getId());
        properties.put("put-db-record-record-reader", avroReaderService.getId());
        properties.put("put-db-record-statement-type", "INSERT");
        properties.put("put-db-record-table-name", PG_TABLE_NAME);
        properties.put("put-db-record-schema-name", GG_SCHEMA_NAME);
        properties.put("put-db-record-unmatched-column-behavior", "Ignore Unmatched Columns");
        getClientUtil().updateProcessorProperties(processor, properties);
        processor = getClientUtil().setAutoTerminatedRelationships(processor,
                Set.of(RELATION_SUCCESS, RELATION_FAILURE, RELATION_RETRY));
        return processor;
    }
}