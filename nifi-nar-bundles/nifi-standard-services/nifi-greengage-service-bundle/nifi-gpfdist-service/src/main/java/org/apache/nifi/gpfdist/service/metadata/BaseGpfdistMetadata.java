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

import org.apache.nifi.gpfdist.metadata.ColumnDescription;
import org.apache.nifi.gpfdist.metadata.ExternalTableFormat;
import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.metadata.TableDescription;

import java.util.List;

public class BaseGpfdistMetadata implements GpfdistMetadata {
    protected final TableDescription tableMetadata;
    protected final List<ColumnDescription> columnDescriptions;
    protected final String externalTable;
    protected final ExternalTableFormat externalTableFormatConfig;
    protected final String gpfdistLocation;

    public BaseGpfdistMetadata(final TableDescription tableMetadata,
                               final List<ColumnDescription> columns,
                               final String externalTable,
                               final ExternalTableFormat externalTableFormatConfig,
                               final String gpfdistLocation) {
        this.tableMetadata = tableMetadata;
        this.columnDescriptions = columns;
        this.externalTable = externalTable;
        this.externalTableFormatConfig = externalTableFormatConfig;
        this.gpfdistLocation = gpfdistLocation;
    }

    @Override
    public TableDescription getTableMetadata() {
        return tableMetadata;
    }

    @Override
    public String getExternalTable() {
        return externalTable;
    }

    @Override
    public ExternalTableFormat getExternalTableFormatConfig() {
        return externalTableFormatConfig;
    }

    @Override
    public String getGpfdistLocation() {
        return gpfdistLocation;
    }

    @Override
    public List<ColumnDescription> getColumnDescriptions() {
        return columnDescriptions;
    }
}
