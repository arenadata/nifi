package org.apache.nifi.gpfdist.service.load.serialization.csv;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.service.datatype.*;
import org.apache.nifi.gpfdist.service.greenplum.model.GreenplumColumnDescription;
import org.apache.nifi.gpfdist.service.load.serialization.RecordSerializationService;
import org.apache.nifi.gpfdist.service.metadata.CsvFormatConfig;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.*;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class CsvRecordSerializationServiceTest {

    private RecordSerializationService serializationService;

    @Test
    void testSerializeAllDataTypes() {
        CsvFormatConfig csvFormatConfig = new CsvFormatConfig();
        ComponentLog logger = mock(ComponentLog.class);
        List<String> fieldNames = List.of("id",
                "f_bigint",
                "f_bit",
                "f_bool",
                "f_bytea",
                "f_char",
                "code",
                "article",
                "f_date",
                "f_float",
                "f_real",
                "f_jsonb",
                "f_numeric",
                "f_double",
                "f_tinyint",
                "f_smallint",
                "f_time",
                "f_timestampz",
                "f_timestamp",
                "f_uuid",
                "f_text_array",
                "f_hstore");
        List<ColumnDescription> columns = List.of(
                new GreenplumColumnDescription(fieldNames.get(0), new IntegerDataType(), true),
                new GreenplumColumnDescription(fieldNames.get(1), new BigintDataType(), false),
                new GreenplumColumnDescription(fieldNames.get(2), new BitDataType(null), false),
                new GreenplumColumnDescription(fieldNames.get(3), new BooleanDataType(), false),
                new GreenplumColumnDescription(fieldNames.get(4), new ByteaDataType(), false),
                new GreenplumColumnDescription(fieldNames.get(5), new CharDataType(2), false),
                new GreenplumColumnDescription(fieldNames.get(6), new VarcharDataType(10), false),
                new GreenplumColumnDescription(fieldNames.get(7), new VarcharDataType(), false),
                new GreenplumColumnDescription(fieldNames.get(8), new DateDataType(), false),
                new GreenplumColumnDescription(fieldNames.get(9), new MoneyDataType(), false),
                new GreenplumColumnDescription(fieldNames.get(10), new RealDataType(), false),
                new GreenplumColumnDescription(fieldNames.get(11), new JsonbDataType(), false),
                new GreenplumColumnDescription(fieldNames.get(12), new DecimalDataType(10, 5), false),
                new GreenplumColumnDescription(fieldNames.get(13), new DoubleDataType(), false),
                new GreenplumColumnDescription(fieldNames.get(14), new SmallintDataType(), false),
                new GreenplumColumnDescription(fieldNames.get(15), new SmallintDataType(), false),
                new GreenplumColumnDescription(fieldNames.get(16), new TimeDataType(6), false),
                new GreenplumColumnDescription(fieldNames.get(17), new TimestampWithTimeZoneDataType(12), false),
                new GreenplumColumnDescription(fieldNames.get(18), new TimestampWithoutTimeZoneDataType(12), false),
                new GreenplumColumnDescription(fieldNames.get(19), new UuidDataType(), false),
                new GreenplumColumnDescription(fieldNames.get(20), new ArrayDataType(new VarcharDataType()), false),
                new GreenplumColumnDescription(fieldNames.get(21), new MapDataType(), false));
        RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField(fieldNames.get(0), RecordFieldType.INT.getDataType()),
                new RecordField(fieldNames.get(1), RecordFieldType.LONG.getDataType()),
                new RecordField(fieldNames.get(2), RecordFieldType.BOOLEAN.getDataType()),
                new RecordField(fieldNames.get(3), RecordFieldType.BOOLEAN.getDataType()),
                new RecordField(fieldNames.get(4), new org.apache.nifi.serialization.record.type.ArrayDataType(RecordFieldType.BYTE.getDataType())),
                new RecordField(fieldNames.get(5), RecordFieldType.STRING.getDataType()),
                new RecordField(fieldNames.get(6), RecordFieldType.STRING.getDataType()),
                new RecordField(fieldNames.get(7), RecordFieldType.STRING.getDataType()),
                new RecordField(fieldNames.get(8), RecordFieldType.DATE.getDataType()),
                new RecordField(fieldNames.get(9), RecordFieldType.DOUBLE.getDataType()),
                new RecordField(fieldNames.get(10), RecordFieldType.FLOAT.getDataType()),
                new RecordField(fieldNames.get(11), RecordFieldType.STRING.getDataType()),
                new RecordField(fieldNames.get(12), RecordFieldType.DECIMAL.getDataType()),
                new RecordField(fieldNames.get(13), RecordFieldType.DOUBLE.getDataType()),
                new RecordField(fieldNames.get(14), RecordFieldType.INT.getDataType()),
                new RecordField(fieldNames.get(15), RecordFieldType.INT.getDataType()),
                new RecordField(fieldNames.get(16), RecordFieldType.TIME.getDataType()),
                new RecordField(fieldNames.get(17), RecordFieldType.TIMESTAMP.getDataType()),
                new RecordField(fieldNames.get(18), RecordFieldType.TIMESTAMP.getDataType()),
                new RecordField(fieldNames.get(19), RecordFieldType.STRING.getDataType()),
                new RecordField(fieldNames.get(20), RecordFieldType.STRING.getDataType()),
                new RecordField(fieldNames.get(21), RecordFieldType.STRING.getDataType())
        ));
        List<Record> records = List.of(
                new MapRecord(recordSchema, new HashMap<>() {{
                    put(fieldNames.get(0), 1);
                    put(fieldNames.get(1), 2478701872L);
                    put(fieldNames.get(2), false);
                    put(fieldNames.get(3), true);
                    put(fieldNames.get(4), new Byte[]{-48, 120});
                    put(fieldNames.get(5), "tt");
                    put(fieldNames.get(6), "c4ca4238a0");
                    put(fieldNames.get(7), "edc8acddc2e9a0a6aec79ddd681c75ac");
                    put(fieldNames.get(8), new Date(1739984400000L));
                    put(fieldNames.get(9), 0.557235836982727);
                    put(fieldNames.get(10), 6.559277);
                    put(fieldNames.get(11), "{\"a\": \"b\"}");
                    put(fieldNames.get(12), 45.51123);
                    put(fieldNames.get(13), 10.3);
                    put(fieldNames.get(14), 15);
                    put(fieldNames.get(15), 5000);
                    put(fieldNames.get(16), new Time(79073375));
                    put(fieldNames.get(17), new Timestamp(1740027473375L));
                    put(fieldNames.get(18), new Timestamp(1740002273375L));
                    put(fieldNames.get(19), "c2142fe5-e305-42ab-8b95-598567e9ea86");
                    put(fieldNames.get(20), "{val, val}");
                    put(fieldNames.get(21), "{ISBN-13=978-1449370000, weight=11.2 ounces, paperback=243, publisher=postgresqltutorial.com, language=English}");
                }}, true, false),
                new MapRecord(recordSchema, new HashMap<>() {{
                    put(fieldNames.get(0), 1);
                    put(fieldNames.get(0), null);
                    put(fieldNames.get(2), null);
                    put(fieldNames.get(3), null);
                    put(fieldNames.get(4), null);
                    put(fieldNames.get(5), null);
                    put(fieldNames.get(6), null);
                    put(fieldNames.get(7), null);
                    put(fieldNames.get(8), null);
                    put(fieldNames.get(9), null);
                    put(fieldNames.get(10), null);
                    put(fieldNames.get(11), null);
                    put(fieldNames.get(12), null);
                    put(fieldNames.get(13), null);
                    put(fieldNames.get(14), null);
                    put(fieldNames.get(15), null);
                    put(fieldNames.get(16), null);
                    put(fieldNames.get(17), null);
                    put(fieldNames.get(18), null);
                    put(fieldNames.get(19), null);
                    put(fieldNames.get(20), null);
                    put(fieldNames.get(21), null);
                }}, true, false));

        serializationService = new CsvRecordSerializationService(recordSchema, columns, csvFormatConfig, logger);
        records.forEach(r -> serializationService.append(r));
        String result = new String(serializationService.toByteArray(), StandardCharsets.UTF_8);
        assertEquals("\"1\"|\"2478701872\"|\"0\"|\"true\"|\"\\xd078\"|\"tt\"|\"c4ca4238a0\"|\"edc8acddc2e9a0a6aec79ddd681c75ac\"|\"2025-02-20\"|\"0.557235836982727\"|\"6.559277\"|\"{\"\"a\"\": \"\"b\"\"}\"|\"45.51123\"|\"10.3\"|\"15\"|\"5000\"|\"04:57:53.000375\"|\"2025-02-20 11:57:53.000375+07:00\"|\"2025-02-20 04:57:53.000375\"|\"c2142fe5-e305-42ab-8b95-598567e9ea86\"|\"{val, val}\"|\"\"\"ISBN-13\"\"=>\"\"978-1449370000\"\", \"\"weight\"\"=>\"\"11.2 ounces\"\", \"\"paperback\"\"=>\"\"243\"\", \"\"publisher\"\"=>\"\"postgresqltutorial.com\"\", \"\"language\"\"=>\"\"English\"\"\"\r\n|||||||||||||||||||||\r\n", result);
    }

    @Test
    void testArrayDataTypeWithStringArray() {
        CsvFormatConfig csvFormatConfig = new CsvFormatConfig();
        ComponentLog logger = mock(ComponentLog.class);
        List<String> fieldNames = List.of(
                "f_text_array");
        List<ColumnDescription> columns = List.of(
                new GreenplumColumnDescription(fieldNames.get(0), new ArrayDataType(new VarcharDataType()), false));
        RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField(fieldNames.get(0), new org.apache.nifi.serialization.record.type.ArrayDataType(RecordFieldType.STRING.getDataType()))
        ));
        List<Record> records = List.of(
                new MapRecord(recordSchema, Map.ofEntries(
                        Map.entry(fieldNames.get(0), new String[]{"val", "val"})
                ), true, false));

        serializationService = new CsvRecordSerializationService(recordSchema, columns, csvFormatConfig, logger);
        records.forEach(r -> serializationService.append(r));
        String result = new String(serializationService.toByteArray(), StandardCharsets.UTF_8);
        assertEquals("\"{\"\"val\"\", \"\"val\"\"}\"\r\n", result);
    }

    @Test
    void testArrayDataTypeWithByteArray() {
        CsvFormatConfig csvFormatConfig = new CsvFormatConfig();
        ComponentLog logger = mock(ComponentLog.class);
        List<String> fieldNames = List.of(
                "f_text_array");
        List<ColumnDescription> columns = List.of(
                new GreenplumColumnDescription(fieldNames.get(0), new ArrayDataType(new VarcharDataType()), false));
        RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField(fieldNames.get(0), new org.apache.nifi.serialization.record.type.ArrayDataType(RecordFieldType.BYTE.getDataType()))
        ));
        List<Record> records = List.of(
                new MapRecord(recordSchema, Map.ofEntries(
                        Map.entry(fieldNames.get(0), ArrayUtils.toObject("{val, val}".getBytes()))
                ), true, false));

        serializationService = new CsvRecordSerializationService(recordSchema, columns, csvFormatConfig, logger);
        records.forEach(r -> serializationService.append(r));
        String result = new String(serializationService.toByteArray(), StandardCharsets.UTF_8);
        assertEquals("\"{\"\"val\"\", \"\"val\"\"}\"\r\n", result);
    }

    @Test
    void testMapDataTypeWithMapOfStrings() {
        CsvFormatConfig csvFormatConfig = new CsvFormatConfig();
        ComponentLog logger = mock(ComponentLog.class);
        List<String> fieldNames = List.of(
                "f_hstore");
        List<ColumnDescription> columns = List.of(
                new GreenplumColumnDescription(fieldNames.get(0), new MapDataType(), false));
        RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField(fieldNames.get(0), new org.apache.nifi.serialization.record.type.MapDataType(RecordFieldType.STRING.getDataType()))
        ));
        List<Record> records = List.of(
                new MapRecord(recordSchema, Map.ofEntries(
                        Map.entry(fieldNames.get(0), new TreeMap<>(Map.of("val", "val", "val2", "val2")))
                ), true, false));

        serializationService = new CsvRecordSerializationService(recordSchema, columns, csvFormatConfig, logger);
        records.forEach(r -> serializationService.append(r));
        String result = new String(serializationService.toByteArray(), StandardCharsets.UTF_8);
        assertEquals("\"\"\"val\"\"=>\"\"val\"\", \"\"val2\"\"=>\"\"val2\"\"\"\r\n", result);
    }

    @Test
    void testMapDataTypeWithMapAsString() {
        CsvFormatConfig csvFormatConfig = new CsvFormatConfig();
        ComponentLog logger = mock(ComponentLog.class);
        List<String> fieldNames = List.of(
                "f_hstore");
        List<ColumnDescription> columns = List.of(
                new GreenplumColumnDescription(fieldNames.get(0), new MapDataType(), false));
        RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField(fieldNames.get(0), RecordFieldType.STRING.getDataType())
        ));
        List<Record> records = List.of(
                new MapRecord(recordSchema, Map.ofEntries(
                        Map.entry(fieldNames.get(0), "{val=val, val2=val2}")
                ), true, false));

        serializationService = new CsvRecordSerializationService(recordSchema, columns, csvFormatConfig, logger);
        records.forEach(r -> serializationService.append(r));
        String result = new String(serializationService.toByteArray(), StandardCharsets.UTF_8);
        assertEquals("\"\"\"val\"\"=>\"\"val\"\", \"\"val2\"\"=>\"\"val2\"\"\"\r\n", result);
    }

    @Test
    void testByteDataTypeWithByteAsString() {
        CsvFormatConfig csvFormatConfig = new CsvFormatConfig();
        ComponentLog logger = mock(ComponentLog.class);
        List<String> fieldNames = List.of(
                "f_byte");
        List<ColumnDescription> columns = List.of(
                new GreenplumColumnDescription(fieldNames.get(0), new ByteaDataType(), false));
        RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField(fieldNames.get(0), RecordFieldType.STRING.getDataType())
        ));
        List<Record> records = List.of(
                new MapRecord(recordSchema, Map.ofEntries(
                        Map.entry(fieldNames.get(0), "\\xd078")
                ), true, false));

        serializationService = new CsvRecordSerializationService(recordSchema, columns, csvFormatConfig, logger);
        records.forEach(r -> serializationService.append(r));
        String result = new String(serializationService.toByteArray(), StandardCharsets.UTF_8);
        assertEquals("\"\\xd078\"\r\n", result);
    }

    @Test
    void testByteDataTypeInvalidInputType() {
        CsvFormatConfig csvFormatConfig = new CsvFormatConfig();
        ComponentLog logger = mock(ComponentLog.class);
        List<String> fieldNames = List.of(
                "f_byte");
        List<ColumnDescription> columns = List.of(
                new GreenplumColumnDescription(fieldNames.get(0), new ByteaDataType(), false));
        RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField(fieldNames.get(0), RecordFieldType.INT.getDataType())
        ));
        List<Record> records = List.of(
                new MapRecord(recordSchema, Map.ofEntries(
                        Map.entry(fieldNames.get(0), 100)
                ), true, false));

        serializationService = new CsvRecordSerializationService(recordSchema, columns, csvFormatConfig, logger);
        assertThrows(RuntimeException.class, () -> records.forEach(r -> serializationService.append(r)), "Unsupported field type: byte for column type BYTEA");
    }

    @Test
    void testArrayDataTypeInvalidInputType() {
        CsvFormatConfig csvFormatConfig = new CsvFormatConfig();
        ComponentLog logger = mock(ComponentLog.class);
        List<String> fieldNames = List.of(
                "f_array");
        List<ColumnDescription> columns = List.of(
                new GreenplumColumnDescription(fieldNames.get(0), new ArrayDataType(new IntegerDataType()), false));
        RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField(fieldNames.get(0), RecordFieldType.INT.getDataType())
        ));
        List<Record> records = List.of(
                new MapRecord(recordSchema, Map.ofEntries(
                        Map.entry(fieldNames.get(0), 100)
                ), true, false));

        serializationService = new CsvRecordSerializationService(recordSchema, columns, csvFormatConfig, logger);
        assertThrows(RuntimeException.class, () -> records.forEach(r -> serializationService.append(r)), "Unsupported field type: int for column type ARRAY");
    }

    @Test
    void testMapDataTypeInvalidInputType() {
        CsvFormatConfig csvFormatConfig = new CsvFormatConfig();
        ComponentLog logger = mock(ComponentLog.class);
        List<String> fieldNames = List.of(
                "f_hstore");
        List<ColumnDescription> columns = List.of(
                new GreenplumColumnDescription(fieldNames.get(0), new MapDataType(), false));
        RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField(fieldNames.get(0), RecordFieldType.INT.getDataType())
        ));
        List<Record> records = List.of(
                new MapRecord(recordSchema, Map.ofEntries(
                        Map.entry(fieldNames.get(0), 100)
                ), true, false));

        serializationService = new CsvRecordSerializationService(recordSchema, columns, csvFormatConfig, logger);
        assertThrows(RuntimeException.class, () -> records.forEach(r -> serializationService.append(r)), "Unsupported field type: int for column type MAP");
    }

    @Test
    void testArrayDataTypeWithInvalidInputArrayElementType() {
        CsvFormatConfig csvFormatConfig = new CsvFormatConfig();
        ComponentLog logger = mock(ComponentLog.class);
        List<String> fieldNames = List.of(
                "f_array");
        List<ColumnDescription> columns = List.of(
                new GreenplumColumnDescription(fieldNames.get(0), new ArrayDataType(new VarcharDataType()), false));
        RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField(fieldNames.get(0), new org.apache.nifi.serialization.record.type.ArrayDataType(new org.apache.nifi.serialization.record.type.ArrayDataType(RecordFieldType.STRING.getDataType())))
        ));
        List<Record> records = List.of(
                new MapRecord(recordSchema, Map.ofEntries(
                        Map.entry(fieldNames.get(0), new String[][]{})
                ), true, false));

        serializationService = new CsvRecordSerializationService(recordSchema, columns, csvFormatConfig, logger);
        assertThrows(RuntimeException.class, () -> records.forEach(r -> serializationService.append(r)), "Unsupported field array element type: string for column type ARRAY");
    }
}