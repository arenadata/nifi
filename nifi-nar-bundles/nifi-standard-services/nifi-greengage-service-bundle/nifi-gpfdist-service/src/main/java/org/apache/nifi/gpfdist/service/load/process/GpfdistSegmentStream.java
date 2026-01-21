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
package org.apache.nifi.gpfdist.service.load.process;

import org.apache.nifi.gpfdist.metadata.RecordProcessorId;
import org.apache.nifi.logging.ComponentLog;

import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class GpfdistSegmentStream {
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicBoolean drainingScheduled = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AsyncContext asyncCtx;
    private final ServletOutputStream outputStream;
    private final RecordProcessorId recordProcessorId;
    private final int avgGpfdistPacketSize;
    private final int capacity;
    private final ArrayBlockingQueue<byte[]> queue;
    private final long enqueueTimeoutMs;
    private final ComponentLog logger;

    public GpfdistSegmentStream(AsyncContext asyncCtx,
                                ServletOutputStream outputStream,
                                RecordProcessorId recordProcessorId,
                                long maxBufferedBytes,
                                int avgGpfdistPacketSize,
                                long enqueueTimeoutMs,
                                ComponentLog logger) {
        this.asyncCtx = asyncCtx;
        this.outputStream = outputStream;
        this.logger = logger;
        this.recordProcessorId = recordProcessorId;
        if (maxBufferedBytes <= 0) {
            throw new IllegalArgumentException("maxBufferedBytes must be > 0");
        }
        if (avgGpfdistPacketSize <= 0) {
            throw new IllegalArgumentException("avgPacketSize must be > 0");
        }
        if (enqueueTimeoutMs < 0) {
            throw new IllegalArgumentException("enqueueTimeoutMs must be >= 0");
        }
        this.avgGpfdistPacketSize = avgGpfdistPacketSize;
        this.enqueueTimeoutMs = enqueueTimeoutMs;

        long queueCapacity = maxBufferedBytes / this.avgGpfdistPacketSize;
        if (queueCapacity < 1) {
            queueCapacity = 1;
        }
        if (queueCapacity > Integer.MAX_VALUE) {
            queueCapacity = Integer.MAX_VALUE;
        }
        this.capacity = (int) queueCapacity;
        this.queue = new ArrayBlockingQueue<>(this.capacity);
        logger.info("Initialized segment stream. recordProcessorId={} maxBufferedBytes={} avgGpfdistPacketSize={} capacity={}",
                recordProcessorId, maxBufferedBytes, avgGpfdistPacketSize, capacity);
    }

    public boolean offer(byte[] packet) {
        if (completed.get() || closed.get()) {
            return false;
        }
        try {
            boolean isActive = queue.offer(packet, enqueueTimeoutMs, TimeUnit.MILLISECONDS);
            if (!isActive) {
                logger.warn("Stream buffer overflow/backpressure. recordProcessorId={} bufferedBytes={} size={}/{}",
                        recordProcessorId, getBytesBuffered(), queue.size(), capacity);
                return false;
            }
            if (closed.get()) {
                queue.remove(packet);
                return false;
            }
            scheduleDrain();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private long getBytesBuffered() {
        return (long) queue.size() * (long) avgGpfdistPacketSize;
    }

    public void complete() {
        if (completed.compareAndSet(false, true)) {
            closed.set(true);
            scheduleDrain();
        }
    }

    private void scheduleDrain() {
        if (!drainingScheduled.compareAndSet(false, true)) {
            return;
        }

        asyncCtx.start(() -> {
            try {
                drain();
            } finally {
                drainingScheduled.set(false);
                if (!queue.isEmpty()) {
                    scheduleDrain();
                } else if (completed.get()) {
                    safeComplete();
                }
            }
        });
    }

    public void drain() {
        if (closed.get() && queue.isEmpty()) {
            safeComplete();
            return;
        }
        try {
            while (outputStream.isReady()) {
                byte[] polled = queue.poll();
                if (polled == null) {
                    break;
                }
                outputStream.write(polled);
            }
            if (completed.get() && queue.isEmpty()) {
                try {
                    outputStream.flush();
                } catch (Exception ignore) {
                }
                safeComplete();
            }
        } catch (Exception e) {
            logger.warn("Drain failed (client disconnect?) recordProcessorId={} err={}", recordProcessorId, e.getMessage());
            safeComplete();
        }
    }

    private void safeComplete() {
        try {
            asyncCtx.complete();
        } catch (Exception ignore) {
        }
    }
}
