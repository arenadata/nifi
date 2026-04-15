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
package org.apache.nifi.gpfdist.service.unload.query;

import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.metadata.GreengageDataType;
import org.apache.nifi.gpfdist.service.metadata.ExternalTableType;
import org.apache.nifi.gpfdist.service.query.AbstractExternalTableQueryFactory;
import org.apache.nifi.gpfdist.metadata.ColumnDescription;

public class CreateWritableExternalTableQueryFactory extends AbstractExternalTableQueryFactory {
    @Override
    public String createQuery(final GpfdistMetadata metadata) {
        return createCommonQuery(metadata);
    }

    @Override
    public ExternalTableType getExternalTableType() {
        return ExternalTableType.WRITABLE;
    }

    @Override
    protected String resolveTypeName(final ColumnDescription columnDescription, final String baseTypeName) {
        // for the money data type, we create a column with the decimal type;
        // the conversion will occur automatically when inserting data into an external table.
        if (columnDescription.getDataType().getType() == GreengageDataType.MONEY) {
            return GreengageDataType.DECIMAL.name();
        }
        return baseTypeName;
    }
}
