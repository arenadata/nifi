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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.nifi.gpfdist.service.RecordProcessor;
import org.apache.nifi.gpfdist.service.RecordProcessorProvider;

import static java.lang.String.format;

public class GpfdistRecordProcessorProvider implements RecordProcessorProvider {
    private static final long GREENPLUM_SEGMENT_WAIT_TIMEOUT = 60000L;

    private final Set<RecordProcessor> registeredProcessors = new HashSet<>();
    private final Queue<RecordProcessor> recordProcessors = new LinkedList<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition isReadyForProcessingCondition = lock.newCondition();

    private boolean isNeedMoreProcessors = true;
    private boolean isReadyForProcessing;

    @Override
    public boolean register(RecordProcessor processor) {
        lock.lock();
        try {
            registeredProcessors.add(processor);
            return addInternal(processor);
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
    public void close() {
        lock.lock();
        try {
            isNeedMoreProcessors = false;
            stopProcessors();
        } finally {
            lock.unlock();
        }
    }

    private void add(final RecordProcessor processor) {
        lock.lock();
        try {
            addInternal(processor);
        } finally {
            lock.unlock();
        }
    }

    private boolean addInternal(final RecordProcessor processor) {
        if (isNeedMoreProcessors) {
            recordProcessors.add(processor);
            isReadyForProcessingCondition.signalAll();
        }
        return isNeedMoreProcessors;
    }

    private RecordProcessor take() {
        lock.lock();
        try {
            long startTime = System.currentTimeMillis();
            while (recordProcessors.isEmpty()) {
                try {
                    if (!isReadyForProcessing &&
                        currentTimeMsProvider().get() - startTime > GREENPLUM_SEGMENT_WAIT_TIMEOUT) {
                        throw new RuntimeException(
                            format("Timeout :%d ms waiting for segments responses is exceeded",
                                GREENPLUM_SEGMENT_WAIT_TIMEOUT));
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

    private void stopProcessors() {
        StringBuilder errorMessages = new StringBuilder();
        registeredProcessors.forEach(processor -> {
            try {
                processor.stop();
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
