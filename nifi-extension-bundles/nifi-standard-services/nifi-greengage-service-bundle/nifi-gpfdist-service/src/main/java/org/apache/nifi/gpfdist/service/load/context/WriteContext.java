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
package org.apache.nifi.gpfdist.service.load.context;

import java.util.Objects;
import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextId;
import org.apache.nifi.gpfdist.service.RecordProcessorProvider;
import org.apache.nifi.gpfdist.service.load.metadata.GpfdistLoadMetadata;
import org.apache.nifi.gpfdist.service.load.metadata.LoadingResult;
import org.apache.nifi.logging.ComponentLog;

public class WriteContext implements Context {
    private final ContextId contextId;
    private final int bufferSize;
    private final GpfdistLoadMetadata metadata;
    private final RecordProcessorProvider recordProcessorProvider;
    private final ComponentLog logger;
    private final LoadingResult result;

    public WriteContext(final ContextId contextId,
                        int bufferSize,
                        final GpfdistLoadMetadata metadata,
                        final RecordProcessorProvider recordProcessorProvider,
                        ComponentLog logger) {
        this.contextId = contextId;
        this.bufferSize = bufferSize;
        this.metadata = metadata;
        this.recordProcessorProvider = recordProcessorProvider;
        this.logger = logger;
        this.result = new LoadingResult();
    }

    @Override
    public ContextId getContextId() {
        return contextId;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public GpfdistLoadMetadata getMetadata() {
        return metadata;
    }

    public LoadingResult getResult() {
        return result;
    }

    public RecordProcessorProvider getRecordProcessorProvider() {
        return recordProcessorProvider;
    }

    public ComponentLog getLogger() {
        return logger;
    }

    @Override
    public void close() {
        recordProcessorProvider.close();
        logger.debug("Closed write context {}", contextId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(contextId);
    }

    @Override
    public String toString() {
        return "WriteContext{" +
                "contextId=" + contextId +
                ", result=" + result +
                '}';
    }
}
