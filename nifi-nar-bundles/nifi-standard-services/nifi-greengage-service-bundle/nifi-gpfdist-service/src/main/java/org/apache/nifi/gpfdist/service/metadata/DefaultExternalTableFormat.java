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
package org.apache.nifi.gpfdist.service.metadata;

import org.apache.nifi.gpfdist.metadata.DataFormat;
import org.apache.nifi.gpfdist.metadata.ExternalTableFormat;

import java.util.Objects;

public class DefaultExternalTableFormat implements ExternalTableFormat {
    private final String delimiter;
    private final String encoding;
    private final String nullValue;
    private final DataFormat dataFormat;

    public DefaultExternalTableFormat(final String delimiter,
                                      final String encoding,
                                      final String nullValue,
                                      final DataFormat dataFormat) {
        this.delimiter = delimiter;
        this.encoding = encoding;
        this.nullValue = nullValue;
        this.dataFormat = dataFormat;
    }

    @Override
    public String getDelimiter() {
        return delimiter;
    }

    @Override
    public String getEncoding() {
        return encoding;
    }

    @Override
    public String getNullValue() {
        return nullValue;
    }

    @Override
    public DataFormat getDataFormat() {
        return dataFormat;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        DefaultExternalTableFormat that = (DefaultExternalTableFormat) o;
        return Objects.equals(delimiter, that.delimiter)
                && Objects.equals(encoding, that.encoding)
                && Objects.equals(nullValue, that.nullValue)
                && dataFormat == that.dataFormat;
    }

    @Override
    public int hashCode() {
        return Objects.hash(delimiter, encoding, nullValue, dataFormat);
    }

    @Override
    public String toString() {
        return "DefaultExternalTableFormat{" +
                "delimiter='" + delimiter + '\'' +
                ", encoding='" + encoding + '\'' +
                ", nullValue='" + nullValue + '\'' +
                ", dataFormat=" + dataFormat +
                '}';
    }
}
