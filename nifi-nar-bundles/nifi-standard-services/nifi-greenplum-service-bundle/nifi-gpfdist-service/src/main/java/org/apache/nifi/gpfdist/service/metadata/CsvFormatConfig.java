package org.apache.nifi.gpfdist.service.metadata;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.nifi.gpfdist.metadata.DataFormat;

import static org.apache.commons.csv.CSVFormat.DEFAULT;

public class CsvFormatConfig implements DataFormatConfig {
    private static final String DEFAULT_ENCODING = "UTF-8";
    private static final CSVFormat GREENPLUM_CSV_FORMAT = DEFAULT.builder()
            .setDelimiter("|")
            .setEscape(null)
            .setIgnoreEmptyLines(false)
            .setQuote(Character.valueOf('"'))
            .setRecordSeparator("\r\n")
            .setNullString("")
            .setQuoteMode(QuoteMode.ALL_NON_NULL)
            .setSkipHeaderRecord(true)
            .build();

    public CsvFormatConfig() {
    }

    public CSVFormat getCsvFormat() {
        return GREENPLUM_CSV_FORMAT;
    }

    public String getEncoding() {
        return DEFAULT_ENCODING;
    }

    @Override
    public DataFormat getDataFormat() {
        return DataFormat.CSV;
    }
}
