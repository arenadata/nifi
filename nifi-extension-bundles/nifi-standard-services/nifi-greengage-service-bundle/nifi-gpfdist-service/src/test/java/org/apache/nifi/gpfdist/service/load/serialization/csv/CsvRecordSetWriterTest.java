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
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CsvRecordSetWriterTest {

    private static final DataType ARRAY_OF_STRING =
            RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());
    private static final DataType ARRAY_OF_BYTE =
            RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
    private static final DataType ARRAY_OF_INT =
            RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType());
    private static final DataType JDBC_ARRAY_CHOICE =
            RecordFieldType.CHOICE.getChoiceDataType(ARRAY_OF_BYTE, ARRAY_OF_STRING);
    private static final DataType STRING_ARRAY_OR_INT_CHOICE =
            RecordFieldType.CHOICE.getChoiceDataType(ARRAY_OF_STRING, RecordFieldType.INT.getDataType());

    @Test
    void choiceResolvesToStringArray() throws IOException {
        RecordField field = new RecordField("val", JDBC_ARRAY_CHOICE);
        String out = write(GreengageDataType.ARRAY, field, new String[]{"a", "b", "c"});

        assertEquals("\"{\"\"a\"\", \"\"b\"\", \"\"c\"\"}\"\n", out);
    }

    @Test
    void choiceResolvesToByteArrayLiteral() throws IOException {
        RecordField field = new RecordField("val", JDBC_ARRAY_CHOICE);
        String out = write(GreengageDataType.ARRAY, field, toByteObjects("{a,b,c}"));

        assertEquals("\"{\"\"a\"\", \"\"b\"\", \"\"c\"\"}\"\n", out);
    }

    @Test
    void choiceUnresolvableForValueThrows() {
        RecordField field = new RecordField("val", JDBC_ARRAY_CHOICE);

        assertThrows(RuntimeException.class, () -> write(GreengageDataType.ARRAY, field, 42));
    }

    @Test
    void choiceResolvesToNonRenderableScalarThrowsAboutThatScalar() {
        // test covers incorrect data case, when type is array but resolved value is not an array
        RecordField field = new RecordField("val", STRING_ARRAY_OR_INT_CHOICE);

        assertThrows(RuntimeException.class, () -> write(GreengageDataType.ARRAY, field, 42));
    }

    @Test
    void choiceResolvesToArrayBranchWithIntChoice() throws IOException {
        RecordField field = new RecordField("val", STRING_ARRAY_OR_INT_CHOICE);
        String out = write(GreengageDataType.ARRAY, field, new String[]{"a", "b", "c"});

        assertEquals("\"{\"\"a\"\", \"\"b\"\", \"\"c\"\"}\"\n", out);
    }

    @Test
    void plainStringArray() throws IOException {
        RecordField field = new RecordField("val", ARRAY_OF_STRING);
        String out = write(GreengageDataType.ARRAY, field, new String[]{"a", "b", "c"});

        assertEquals("\"{\"\"a\"\", \"\"b\"\", \"\"c\"\"}\"\n", out);
    }

    @Test
    void plainIntArray() throws IOException {
        RecordField field = new RecordField("val", ARRAY_OF_INT);
        String out = write(GreengageDataType.ARRAY, field, new Integer[]{1, 2, 3});

        assertEquals("\"{\"\"1\"\", \"\"2\"\", \"\"3\"\"}\"\n", out);
    }

    private String write(GreengageDataType columnType, RecordField field, Object value) throws IOException {
        RecordSchema schema = new SimpleRecordSchema(List.of(field));

        Map<String, Object> values = new HashMap<>();
        values.put(field.getFieldName(), value);
        Record record = new MapRecord(schema, values);

        ColumnDataType columnDataType = mock(ColumnDataType.class);
        when(columnDataType.getType()).thenReturn(columnType);
        ColumnDescription columnDescription = mock(ColumnDescription.class);
        when(columnDescription.getDataType()).thenReturn(columnDataType);

        CSVFormat csvFormat = new CsvFormatConfig().getCsvFormat().builder().setRecordSeparator('\n').build();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (CsvRecordSetWriter writer = new CsvRecordSetWriter(
                csvFormat, schema, baos, false, "UTF-8", List.of(columnDescription), mock(ComponentLog.class))) {
            writer.writeRecord(record);
        }
        return baos.toString(StandardCharsets.UTF_8);
    }

    private static Byte[] toByteObjects(String value) {
        byte[] raw = value.getBytes(StandardCharsets.UTF_8);
        Byte[] boxed = new Byte[raw.length];
        for (int i = 0; i < raw.length; i++) {
            boxed[i] = raw[i];
        }
        return boxed;
    }
}
