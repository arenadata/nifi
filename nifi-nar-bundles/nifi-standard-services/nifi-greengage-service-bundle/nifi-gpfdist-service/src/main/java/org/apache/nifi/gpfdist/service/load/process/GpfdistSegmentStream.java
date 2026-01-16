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
import java.util.concurrent.atomic.AtomicBoolean;

public class GpfdistSegmentStream {
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicBoolean drainingScheduled = new AtomicBoolean(false);
    private final AsyncContext asyncCtx;
    private final ServletOutputStream outputStream;
    private final RecordProcessorId recordProcessorId;
    private final GpfdistBoundedPacketQueue queue;
    private final long enqueueTimeoutMs;
    private final ComponentLog logger;

    public GpfdistSegmentStream(AsyncContext asyncCtx,
                                ServletOutputStream outputStream,
                                RecordProcessorId recordProcessorId,
                                long maxBufferedBytes,
                                long enqueueTimeoutMs,
                                ComponentLog logger) {
        this.asyncCtx = asyncCtx;
        this.outputStream = outputStream;
        this.logger = logger;
        this.recordProcessorId = recordProcessorId;
        this.queue = new GpfdistBoundedPacketQueue(maxBufferedBytes);
        this.enqueueTimeoutMs = enqueueTimeoutMs;
    }

    public boolean offer(byte[] packet) {
        if (completed.get() || queue.isClosed()) {
            return false;
        }
        try {
            boolean isActive = queue.offer(packet, enqueueTimeoutMs);
            if (!isActive) {
                logger.warn("Stream buffer overflow/backpressure. recordProcessorId={} bufferedBytes={}",
                        recordProcessorId, queue.getBytesBuffered());
                return false;
            }
            scheduleDrain();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public void complete() {
        if (completed.compareAndSet(false, true)) {
            queue.close();
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
                if (!queue.isEmpty() || (completed.get() && !queue.isEmpty())) {
                    scheduleDrain();
                }
            }
        });
    }

    public void drain() {
        if (queue.isClosed() && queue.isEmpty()) {
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
