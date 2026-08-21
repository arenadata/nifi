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
package org.apache.nifi.tests.system.arenadata;

import io.qameta.allure.Allure;
import io.qameta.allure.model.Parameter;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;

public class AllureExtension implements BeforeEachCallback {

    @Override
    public void beforeEach(ExtensionContext context) {
        String adbVersion = System.getProperty("adb.version");
        if (adbVersion == null || adbVersion.isEmpty()) {
            return;
        }
        Allure.getLifecycle().updateTestCase(allureResult -> {
            String oldHistoryId = allureResult.getHistoryId();
            allureResult.setHistoryId(oldHistoryId + adbVersion);
            List<Parameter> parameters = new ArrayList<>();
            Parameter adbVersionParameter = new Parameter().setName("adbVersion").setValue("ADB " + adbVersion);
            parameters.add(adbVersionParameter);
            allureResult.setParameters(parameters);
            allureResult.getLabels().removeIf(label -> "suite".equals(label.getName()));
        });
        Allure.suite("Greengage connector: ADB " + adbVersion);
    }
}
