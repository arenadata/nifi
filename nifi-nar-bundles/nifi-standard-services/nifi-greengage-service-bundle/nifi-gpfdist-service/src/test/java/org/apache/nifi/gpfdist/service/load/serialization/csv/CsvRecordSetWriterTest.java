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
package org.apache.nifi.gpfdist.service.load.serialization.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.GreengageDataType;
import org.apache.nifi.gpfdist.service.metadata.CsvFormatConfig;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CsvRecordSetWriterTest {

    @Test
    void recordFieldsMapToColumnsByPosition() throws IOException {
        Record record = new MapRecord(stringSchema("x", "y"), Map.of("x", "first", "y", "second"));

        String out = write(stringSchema("a", "b"), record, List.of(GreengageDataType.VARCHAR, GreengageDataType.VARCHAR));

        assertEquals("\"first\"|\"second\"\n", out);
    }

    @Test
    void recordWithOtherFieldCountThrows() {
        Record record = new MapRecord(stringSchema("x", "y", "z"), Map.of("x", "1", "y", "2", "z", "3"));

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> write(stringSchema("a", "b"), record, List.of(GreengageDataType.VARCHAR, GreengageDataType.VARCHAR)));
        assertTrue(e.getMessage().contains("does not match target column count"), e.getMessage());
    }

    private String write(RecordSchema writerSchema, Record record, List<GreengageDataType> columnTypes) throws IOException {
        List<ColumnDescription> columnDescriptions = columnTypes.stream()
                .map(CsvRecordSetWriterTest::mockColumnDescription)
                .collect(Collectors.toList());
        CSVFormat csvFormat = new CsvFormatConfig().getCsvFormat().builder().setRecordSeparator('\n').build();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (CsvRecordSetWriter writer = new CsvRecordSetWriter(
                csvFormat, writerSchema, baos, false, "UTF-8", columnDescriptions, mock(ComponentLog.class))) {
            writer.writeRecord(record);
        }
        return baos.toString(StandardCharsets.UTF_8);
    }

    private static ColumnDescription mockColumnDescription(GreengageDataType columnType) {
        ColumnDataType columnDataType = mock(ColumnDataType.class);
        when(columnDataType.getType()).thenReturn(columnType);
        ColumnDescription columnDescription = mock(ColumnDescription.class);
        when(columnDescription.getDataType()).thenReturn(columnDataType);
        return columnDescription;
    }

    private static RecordSchema stringSchema(String... fieldNames) {
        return new SimpleRecordSchema(Arrays.stream(fieldNames)
                .map(name -> new RecordField(name, RecordFieldType.STRING.getDataType()))
                .collect(Collectors.toList()));
    }
}
