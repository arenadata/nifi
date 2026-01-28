/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.gpfdist.service.util;

import org.apache.nifi.gpfdist.service.metadata.ExternalTableType;

import java.util.UUID;

public final class GpfdistUtil {
    public static final String WRITE_CONTEXT_MANAGER_ATTR = "writeContextManager";
    public static final String READ_CONTEXT_MANAGER_ATTR = "readContextManager";
    public static final String INPUT_DATA_PROCESSOR_FACTORY_ATTR = "inputDataProcessorFactory";
    public static final String RECORD_PROCESSOR_FACTORY_ATTR = "recordProcessorFactory";
    public static final String RECORD_PROCESSING_EXECUTOR_SERVICE_ATTR = "recordProcessingExecutorService";
    public static final String COMPONENT_LOG_ATTR = "componentLog";

    private GpfdistUtil() {
    }

    public static String createGpfdistFileName(String externalTableName) {
        return "/greengage/" + externalTableName;
    }

    public static String createExternalTableName(ExternalTableType tableType) {
        return String.format("nifi_external_%s_%s", tableType.name().toLowerCase(),
                getUniqueSuffix());
    }

    public static String createExternalTableName(ExternalTableType tableType, int globalWorkerIndex) {
        //nifi_external_writable_<global_worker_index>
        return String.format("nifi_external_%s_%d_%s", tableType.name().toLowerCase(),
                globalWorkerIndex,
                getUniqueSuffix());
    }

    private static String getUniqueSuffix() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
