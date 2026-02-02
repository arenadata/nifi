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
package org.apache.nifi.gpfdist.service.load.process;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextId;
import org.apache.nifi.gpfdist.service.RecordSink;
import org.apache.nifi.gpfdist.service.load.context.WriteContext;
import org.apache.nifi.gpfdist.service.load.context.WriteContextManager;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.Record;

import static java.lang.String.format;

public class GpfdistRecordSink implements RecordSink {
    private final WriteContext writeContext;
    private final ExecutorService executorService;
    private final WriteContextManager contextManager;
    private final Queue<CompletableFuture<Void>> loadingRecordFutureQueue = new ArrayDeque<>();
    private final ComponentLog logger;

    public GpfdistRecordSink(final ContextId contextId,
                             final ExecutorService executorService,
                             final WriteContextManager contextManager,
                             ComponentLog logger) {
        this.contextManager = contextManager;
        this.writeContext = contextManager.get(contextId)
            .orElseThrow(() -> new IllegalArgumentException("No write context found for contextId: " + contextId));
        this.executorService = executorService;
        this.logger = logger;
    }

    @Override
    public void load(Record record) {
        CompletableFuture<Void> recordFuture = CompletableFuture.runAsync(() ->
                writeContext.getRecordProcessorProvider()
                    .useProcessor(processor -> processor.process(record)),
            executorService);
        loadingRecordFutureQueue.add(recordFuture);
    }

    @Override
    public CompletableFuture<Void> finish() {
        return CompletableFuture.allOf(loadingRecordFutureQueue.toArray(new CompletableFuture[0]))
            .thenRunAsync(() -> {
                logger.info("Finished loading records within context {}", writeContext.getContextId());
                writeContext.close();
                contextManager.remove(writeContext.getContextId());
            }, executorService);
    }

    @Override
    public void abort() {
        String errMsg = format("Loading data within context %s is aborted", writeContext.getContextId());
        failContext(new RuntimeException(errMsg));
        logger.warn(errMsg);
    }

    @Override
    public Context getContext() {
        return writeContext;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(writeContext);
    }

    private void failContext(Throwable e) {
        try {
            loadingRecordFutureQueue.clear();
            writeContext.getResult().getError().set(e);
            writeContext.close();
        } finally {
            contextManager.remove(writeContext.getContextId());
        }
    }
}
