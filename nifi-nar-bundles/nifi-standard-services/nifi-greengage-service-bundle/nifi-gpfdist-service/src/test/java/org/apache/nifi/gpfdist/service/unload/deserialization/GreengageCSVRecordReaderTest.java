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

import org.apache.commons.csv.CSVFormat;
import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.service.datatype.TimeDataType;
import org.apache.nifi.gpfdist.service.datatype.TimestampWithTimeZoneDataType;
import org.apache.nifi.gpfdist.service.datatype.TimestampWithoutTimeZoneDataType;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GreengageCSVRecordReaderTest {
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String TIME_FORMAT = "HH:mm:ss.SSSSSS";
    private static final String TIMESTAMP_WITHOUT_TIME_ZONE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private static final String TIMESTAMP_WITH_TIME_ZONE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSSX";

    @Test
    void readTemporalValuesWithVariableFractionPrecision() throws Exception {
        for (int fractionLength = 1; fractionLength <= 6; fractionLength++) {
            final String fraction = "123456".substring(0, fractionLength);
            final int expectedTimeMillis = Integer.parseInt((fraction + "000").substring(0, 3));
            final int expectedTimestampNanos = Integer.parseInt((fraction + "000000000").substring(0, 9));

            assertTemporalValuesParsed(
                    "16:57:05." + fraction,
                    "2026-05-22 14:03:01." + fraction,
                    "2026-05-22 14:03:01." + fraction + "+00",
                    expectedTimeMillis,
                    expectedTimestampNanos);
        }
    }

    private void assertTemporalValuesParsed(final String timeValue,
                                            final String timestampValue,
                                            final String timestamptzValue,
                                            final int expectedTimeMillis,
                                            final int expectedTimestampNanos) throws Exception {
        final String csv = String.join(",", timeValue, timestampValue, timestamptzValue);

        try (GreengageCSVRecordReader reader = new GreengageCSVRecordReader(
                new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)),
                null,
                createSchema(),
                CSVFormat.DEFAULT,
                DATE_FORMAT,
                TIME_FORMAT,
                TIMESTAMP_WITHOUT_TIME_ZONE_FORMAT,
                TIMESTAMP_WITH_TIME_ZONE_FORMAT,
                StandardCharsets.UTF_8.name(),
                false,
                createDataTypes())) {
            final Record record = reader.nextRecord();

            assertTrue(record.getValue("f_time") instanceof Time);
            assertTrue(record.getValue("f_timestamp") instanceof Timestamp);
            assertTrue(record.getValue("f_timestamptz") instanceof Timestamp);

            final Time time = (Time) record.getValue("f_time");
            final Timestamp timestamp = (Timestamp) record.getValue("f_timestamp");
            final Timestamp timestamptz = (Timestamp) record.getValue("f_timestamptz");

            assertEquals(expectedTimeMillis, time.getTime() % 1000);
            assertEquals(LocalDateTime.parse(timestampValue.replace(' ', 'T')), timestamp.toLocalDateTime());
            assertEquals(expectedTimestampNanos, timestamp.getNanos());
            assertEquals(OffsetDateTime.parse(timestamptzValue.replace(' ', 'T') + ":00").toInstant(), timestamptz.toInstant());
            assertEquals(expectedTimestampNanos, timestamptz.getNanos());
        }
    }

    private RecordSchema createSchema() {
        return new SimpleRecordSchema(List.of(
                new RecordField("f_time", RecordFieldType.TIME.getDataType()),
                new RecordField("f_timestamp", RecordFieldType.TIMESTAMP.getDataType()),
                new RecordField("f_timestamptz", RecordFieldType.TIMESTAMP.getDataType())));
    }

    private Map<String, ColumnDataType> createDataTypes() {
        return Map.of(
                "f_time", new TimeDataType(6),
                "f_timestamp", new TimestampWithoutTimeZoneDataType(6),
                "f_timestamptz", new TimestampWithTimeZoneDataType(6));
    }
}
