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

import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextManager;
import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.service.query.InsertDataQueryFactory;
import org.apache.nifi.gpfdist.service.unload.context.ReadContext;
import org.apache.nifi.gpfdist.service.unload.metadata.GpfdistUnloadMetadata;

import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.getFullName;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.quote;

public class UnloadInsertDataQueryFactory implements InsertDataQueryFactory {
    private static final String COLUMN_DELIMITER = ", ";
    private final ContextManager<Context> readContextManager;

    public UnloadInsertDataQueryFactory(ContextManager<Context> readContextManager) {
        this.readContextManager = readContextManager;
    }

    @Override
    public String create(final GpfdistMetadata unloadMetadata) {
        String externalTableColumnNames = unloadMetadata.getColumnDescriptions().stream()
                .map(colDesc -> quote(colDesc.getName()))
                .collect(Collectors.joining(COLUMN_DELIMITER));
        String targetTableColumnNames;
        targetTableColumnNames = externalTableColumnNames;
        return format("INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s",
                unloadMetadata.getExternalTable(),
                targetTableColumnNames,
                externalTableColumnNames,
                getFullName(unloadMetadata.getTableMetadata().getSchemaName(), unloadMetadata.getTableMetadata().getTableName()),
                getWhereCondition(unloadMetadata));
    }

    private String getWhereCondition(GpfdistMetadata metadata) {
        GpfdistUnloadMetadata unloadMetadata = (GpfdistUnloadMetadata) metadata;
        ReadContext readContext = readContextManager.get(unloadMetadata.getContextId())
                .map(context -> (ReadContext) context)
                .orElseThrow(() -> new IllegalArgumentException(String.format("Failed to find read context with id %s",
                        unloadMetadata.getContextId())));
        return String.format("gp_segment_id %% %d = %d",
                readContext.getGlobalParallelFactor(),
                unloadMetadata.getGlobalWorkerIndex());
    }
}
