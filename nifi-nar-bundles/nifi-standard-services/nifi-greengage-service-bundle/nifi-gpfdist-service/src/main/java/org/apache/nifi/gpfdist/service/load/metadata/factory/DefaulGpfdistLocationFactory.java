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
package org.apache.nifi.gpfdist.service.load.metadata.factory;

import org.apache.nifi.gpfdist.metadata.ContextId;
import org.apache.nifi.gpfdist.server.config.GpfdistServerConfig;
import org.apache.nifi.gpfdist.service.metadata.ExternalTableType;
import org.apache.nifi.gpfdist.service.metadata.GpfdistLocationFactory;

import java.util.Objects;

public class DefaulGpfdistLocationFactory
        implements GpfdistLocationFactory {
    private final GpfdistServerConfig config;

    public DefaulGpfdistLocationFactory(final GpfdistServerConfig config) {
        this.config = config;
    }

    @Override
    public String create(ContextId contextId, String processorTaskId, final String externalTableName, final ExternalTableType externalTableType) {
        String protocol = config.isSslEnabled() ? "gpfdists" : "gpfdist";
        //gpfdist://<host>:<port>/gpfdist/<operation>/<contextId>/<processorTaskId>/<externalTable>
        return String.format("%s://%s:%d/gpfdist/%s/%s/%s/%s",
                protocol,
                config.getHost(),
                config.getPort(),
                getOperationPath(externalTableType),
                contextId.getId(),
                processorTaskId,
                externalTableName);
    }

    private String getOperationPath(ExternalTableType externalTableType) {
        if (externalTableType == ExternalTableType.READABLE) {
            return "read";
        } else if (externalTableType == ExternalTableType.WRITABLE) {
            return "write";
        }
        throw new IllegalArgumentException("Unsupported external table type: " + externalTableType);
    }
}
