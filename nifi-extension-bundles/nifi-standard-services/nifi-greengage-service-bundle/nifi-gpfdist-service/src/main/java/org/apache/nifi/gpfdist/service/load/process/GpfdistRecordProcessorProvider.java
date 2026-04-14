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

import org.apache.nifi.gpfdist.metadata.RecordProcessorLoadingResult;
import org.apache.nifi.gpfdist.service.RecordProcessor;
import org.apache.nifi.gpfdist.service.RecordProcessorProvider;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.String.format;

public class GpfdistRecordProcessorProvider implements RecordProcessorProvider {
    private static final long GREENGAGE_SEGMENT_WAIT_TIMEOUT = 60000L;
    private final Set<RecordProcessor> registeredProcessors = new HashSet<>();
    private final Queue<RecordProcessor> recordProcessors = new LinkedList<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition isReadyForProcessingCondition = lock.newCondition();
    private final Collection<RecordProcessorLoadingResult> recordProcessorLoadingResults = new ArrayList<>();
    private final String sinkId;
    private final ComponentLog logger;
    private boolean isReadyForProcessing;
    private ProviderState state = ProviderState.OPEN;

    enum ProviderState {OPEN, CLOSED, ABORTED}

    public GpfdistRecordProcessorProvider(String sinkId, ComponentLog logger) {
        this.sinkId = sinkId;
        this.logger = logger;
    }

    @Override
    public boolean register(RecordProcessor processor) {
        lock.lock();
        try {
            if (state != ProviderState.OPEN) {
                logger.warn("Register processor {} is not allowed. State is not OPEN. actual: {}", processor.getId(), state);
                return false;
            }
            registeredProcessors.add(processor);
            recordProcessorLoadingResults.add(processor.getResult());
            recordProcessors.add(processor);
            isReadyForProcessingCondition.signalAll();
            logger.debug("Registered processor {}. for sinkId {}", processor.getId(), sinkId);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void useProcessor(Consumer<RecordProcessor> action) {
        RecordProcessor processor = take();
        action.accept(processor);
        add(processor);
    }

    @Override
    public void stop() {
        lock.lock();
        try {
            if (state == ProviderState.OPEN) {
                state = ProviderState.CLOSED;
            }
            isReadyForProcessingCondition.signalAll();
            stop(null);
            logger.debug("Stopped all processors for sinkId {}", sinkId);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void abort() {
        lock.lock();
        try {
            state = ProviderState.ABORTED;
            isReadyForProcessingCondition.signalAll();
            stop(new RuntimeException("Record processor provider has been aborted"));
            logger.debug("Abort all processors for sinkId {}", sinkId);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void reset() {
        lock.lock();
        try {
            state = ProviderState.OPEN;
            isReadyForProcessing = false;
            if (!registeredProcessors.isEmpty()) {
                stop(null);
            }
            recordProcessorLoadingResults.clear();
            logger.debug("Reset all processors for sinkId {}", sinkId);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Collection<RecordProcessorLoadingResult> getResult() {
        return recordProcessorLoadingResults;
    }

    private void add(final RecordProcessor processor) {
        lock.lock();
        try {
            if (state != ProviderState.OPEN) {
                return;
            }
            recordProcessors.add(processor);
            isReadyForProcessingCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private RecordProcessor take() {
        lock.lock();
        try {
            long startTime = System.currentTimeMillis();
            while (recordProcessors.isEmpty()) {
                if (state != ProviderState.OPEN) {
                    throw new RuntimeException("Provider is not OPEN while waiting for processor. State=" + state);
                }
                try {
                    if (!isReadyForProcessing
                            && currentTimeMsProvider().get() - startTime > GREENGAGE_SEGMENT_WAIT_TIMEOUT) {
                        throw new RuntimeException(
                                format("Timeout :%d ms waiting for segments responses is exceeded",
                                        GREENGAGE_SEGMENT_WAIT_TIMEOUT));
                    }
                    isReadyForProcessingCondition.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            isReadyForProcessing = true;
            return recordProcessors.poll();
        } finally {
            lock.unlock();
        }
    }

    private void stop(Throwable error) {
        StringBuilder errorMessages = new StringBuilder();
        registeredProcessors.forEach(processor -> {
            try {
                if (error != null) {
                    processor.stopExceptionally(error);
                } else {
                    processor.stop();
                }
            } catch (Exception e) {
                errorMessages.append(
                        format("Failed to stop record processor %s. Error: %s;", processor, e.getMessage()));
            }
        });
        registeredProcessors.clear();
        recordProcessors.clear();
        if (errorMessages.length() != 0) {
            throw new RuntimeException(errorMessages.toString());
        }
    }

    public static Supplier<Long> currentTimeMsProvider() {
        return System::currentTimeMillis;
    }
}
