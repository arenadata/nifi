package org.apache.nifi.gpfdist.service.load.process;

import org.apache.nifi.gpfdist.service.RecordProcessor;
import org.apache.nifi.gpfdist.service.RecordProcessorProvider;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static java.lang.String.format;

public class GpfdistRecordProcessorProvider implements RecordProcessorProvider {
    private static final long GREENPLUM_SEGMENT_WAIT_TIMEOUT = 60000L;
    private final Queue<RecordProcessor> recordProcessors = new LinkedList<>();
    private final AtomicBoolean isReadyForProcessing = new AtomicBoolean(false);
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition isReadyForProcessingCondition = lock.newCondition();
    private boolean isNeedMoreProcessors = true;

    @Override
    public boolean add(final RecordProcessor processor) {
        lock.lock();
        try {
            if (isNeedMoreProcessors) {
                recordProcessors.add(processor);
                isReadyForProcessingCondition.signalAll();
            }
            return isNeedMoreProcessors;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public RecordProcessor take() {
        lock.lock();
        try {
            long startTime = System.currentTimeMillis();
            while (recordProcessors.isEmpty()) {
                try {
                    if (!isReadyForProcessing.get() && currentTimeMsProvider().get() - startTime > GREENPLUM_SEGMENT_WAIT_TIMEOUT) {
                        throw new RuntimeException(
                                format("Timeout :%d ms waiting for segments responses is exceeded",
                                        GREENPLUM_SEGMENT_WAIT_TIMEOUT));
                    }
                    isReadyForProcessingCondition.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            isReadyForProcessing.set(true);
            return recordProcessors.poll();
        } finally {
            lock.unlock();
        }
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

    private void stopProcessors() {
        StringBuilder sb = new StringBuilder();
        recordProcessors.forEach(processor -> {
            try {
                processor.stop();
            } catch (Exception e) {
                sb.append(format("Failed to stop record processor %s. Error: %s;", processor, e.getMessage()));
            }
        });
        recordProcessors.clear();
        if (sb.length() != 0) {
            throw new RuntimeException(sb.toString());
        }
    }

    public static Supplier<Long> currentTimeMsProvider() {
        return System::currentTimeMillis;
    }
}
