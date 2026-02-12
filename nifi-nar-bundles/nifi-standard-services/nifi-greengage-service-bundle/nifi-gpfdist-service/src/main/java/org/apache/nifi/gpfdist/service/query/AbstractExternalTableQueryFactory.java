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
package org.apache.nifi.gpfdist.service.query;

import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.metadata.GreengageDataType;
import org.apache.nifi.gpfdist.service.datatype.EnumDataType;

import java.util.Objects;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.quote;

public abstract class AbstractExternalTableQueryFactory implements CreateExternalTableQueryFactory {

    protected String createCommonQuery(final GpfdistMetadata metadata) {
        return format(
                "CREATE %s EXTERNAL TABLE %s (%s) LOCATION ('%s') FORMAT '%s' (DELIMITER '%s' NULL AS '%s') ENCODING '%s'",
                getExternalTableType().name(),
                metadata.getExternalTable(),
                getColumnDefinition(metadata),
                metadata.getGpfdistLocation(),
                metadata.getExternalTableFormatConfig().getDataFormat().name(),
                metadata.getExternalTableFormatConfig().getDelimiter(),
                metadata.getExternalTableFormatConfig().getNullValue(),
                metadata.getExternalTableFormatConfig().getEncoding());
    }

    protected String getColumnDefinition(GpfdistMetadata metadata) {
        return IntStream.range(0, metadata.getColumnDescriptions().size())
                .boxed()
                .map(i -> {
                    ColumnDescription columnDescription = metadata.getColumnDescriptions().get(i);
                    return quote(columnDescription.getName()) + " " + getTypeName(columnDescription);
                })
                .collect(joining(","));
    }

    private String getTypeName(ColumnDescription columnDescription) {
        if (Objects.requireNonNull(columnDescription.getDataType().getType()) == GreengageDataType.ENUM) {
            EnumDataType enumDataType = (EnumDataType) columnDescription.getDataType();
            return enumDataType.getEnumTypeSchema() + "." + enumDataType.getEnumTypeName();
        }
        return columnDescription.getDataType().getName();
    }
}
