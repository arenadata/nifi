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
package org.apache.nifi.gpfdist.service.greengage.model;

import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.metadata.ColumnDescription;

import java.util.Objects;

public class GreengageColumnDescription implements ColumnDescription {
    private final String columnName;
    private final ColumnDataType dataType;
    private final boolean required;
    private final boolean isNullable;

    public GreengageColumnDescription(final String columnName,
                                      final ColumnDataType dataType,
                                      final boolean required,
                                      boolean isNullable) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.required = required;
        this.isNullable = isNullable;
    }

    @Override
    public String getName() {
        return columnName;
    }

    @Override
    public ColumnDataType getDataType() {
        return dataType;
    }

    @Override
    public boolean isRequired() {
        return required;
    }

    @Override
    public boolean isNullable() {
        return isNullable;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GreengageColumnDescription that = (GreengageColumnDescription) o;
        return required == that.required && Objects.equals(columnName, that.columnName) && Objects.equals(dataType, that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, dataType, required);
    }

    @Override
    public String toString() {
        return "GreengageColumnDescription{" +
                "columnName='" + columnName + '\'' +
                ", dataType=" + dataType +
                ", required=" + required +
                '}';
    }
}
