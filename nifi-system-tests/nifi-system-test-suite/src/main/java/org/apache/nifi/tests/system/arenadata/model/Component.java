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
package org.apache.nifi.tests.system.arenadata.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum Component {
    MDW("mdw", 6000),
    SDW1("sdw1", 0),
    SDW2("sdw2", 0),
    SMDW("smdw", 0),
    POSTGRES("postgres", 5432),
    ZOOKEEPER("zookeeper", 0),
    NIFI01("nifi01", 6980),
    NIFI02("nifi02", 6979),
    NIFI03("nifi03", 6978);

    private final String name;
    private final int port;
}
