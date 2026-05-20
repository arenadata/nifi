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
package org.apache.nifi.gpfdist.service.unload.context;

import org.apache.nifi.gpfdist.metadata.ColumnDataType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GreengageTableColumnsMaxValueContext {
    private final List<String> maxValueColumnNamesList;
    private final Map<String, ColumnDataType> maxValueColumnTypes;
    private final Map<String, String> lowerValues;
    private final Map<String, String> upperValues;

    public GreengageTableColumnsMaxValueContext(final List<String> maxValueColumnNamesList,
                                                final Map<String, ColumnDataType> maxValueColumnTypes,
                                                final Map<String, String> lowerValues,
                                                final Map<String, String> upperValues) {
        this.maxValueColumnNamesList = List.copyOf(maxValueColumnNamesList);
        this.maxValueColumnTypes = Collections.unmodifiableMap(new LinkedHashMap<>(maxValueColumnTypes));
        this.lowerValues = Collections.unmodifiableMap(new LinkedHashMap<>(lowerValues));
        this.upperValues = Collections.unmodifiableMap(new LinkedHashMap<>(upperValues));
    }

    public List<String> getMaxValueColumnNamesList() {
        return maxValueColumnNamesList;
    }

    public Map<String, ColumnDataType> getMaxValueColumnTypes() {
        return maxValueColumnTypes;
    }

    public Map<String, String> getLowerValues() {
        return lowerValues;
    }

    public Map<String, String> getUpperValues() {
        return upperValues;
    }
}
