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

import org.apache.nifi.gpfdist.metadata.ColumnDataType;
import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextManager;
import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;
import org.apache.nifi.gpfdist.service.query.InsertDataQueryFactory;
import org.apache.nifi.gpfdist.service.unload.context.ReadContext;
import org.apache.nifi.gpfdist.service.unload.metadata.GpfdistUnloadMetadata;
import org.apache.nifi.gpfdist.service.util.GreengageUtil;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.getFullName;
import static org.apache.nifi.gpfdist.service.util.GreengageUtil.getLiteralByType;
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
        final String segmentPredicate = String.format("gp_segment_id %% %d = %d",
                readContext.getGlobalParallelFactor(),
                unloadMetadata.getGlobalWorkerIndex());
        final String maxValuePredicate = readContext.getTableColumnsMaxValueContext(unloadMetadata.getProcessorTaskId())
                .map(ctx -> buildMaxValuePredicate(ctx.getMaxValueColumnNamesList(),
                        ctx.getMaxValueColumnTypes(),
                        ctx.getLowerValues(),
                        ctx.getUpperValues()))
                .orElse(null);
        if (maxValuePredicate == null || maxValuePredicate.isBlank()) {
            return segmentPredicate;
        }
        return segmentPredicate + " AND " + maxValuePredicate;
    }

    private String buildMaxValuePredicate(final List<String> maxValueColumnNamesList,
                                          final Map<String, ColumnDataType> maxValueColumnTypes,
                                          final Map<String, String> lowerValues,
                                          final Map<String, String> upperValues) {
        if (maxValueColumnNamesList.isEmpty()) {
            return "";
        }
        final String tupleColumns = maxValueColumnNamesList.stream()
                .map(GreengageUtil::quote)
                .collect(Collectors.joining(COLUMN_DELIMITER, "(", ")"));
        final StringBuilder clauseBuilder = new StringBuilder();

        if (hasAllValues(maxValueColumnNamesList, lowerValues)) {
            final String lowerTuple = maxValueColumnNamesList.stream()
                    .map(columnName -> getLiteralByType(maxValueColumnTypes, columnName, lowerValues.get(columnName)))
                    .collect(Collectors.joining(COLUMN_DELIMITER, "(", ")"));
            clauseBuilder.append(tupleColumns).append(" > ").append(lowerTuple);
        }
        if (hasAllValues(maxValueColumnNamesList, upperValues)) {
            final String upperTuple = maxValueColumnNamesList.stream()
                    .map(columnName -> getLiteralByType(maxValueColumnTypes, columnName, upperValues.get(columnName)))
                    .collect(Collectors.joining(COLUMN_DELIMITER, "(", ")"));
            if (clauseBuilder.length() > 0) {
                clauseBuilder.append(" AND ");
            }
            clauseBuilder.append(tupleColumns).append(" <= ").append(upperTuple);
        }
        return clauseBuilder.toString();
    }

    private boolean hasAllValues(final List<String> columns, final Map<String, String> values) {
        return columns.stream().allMatch(column -> values.get(column) != null);
    }
}
