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
package org.apache.nifi.gpfdist.service.unload.process;


import org.apache.nifi.gpfdist.service.unload.dto.GreengageChunkId;
import org.apache.nifi.gpfdist.service.unload.dto.UnloadingResult;
import org.apache.nifi.logging.ComponentLog;

import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class GpfdistChunkRequestProcessor {
    private final GreengageChunkId chunkId;
    private final InputDataProcessor dataProcessor;
    private final ComponentLog logger;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public GpfdistChunkRequestProcessor(GreengageChunkId chunkId,
                                        InputDataProcessor dataProcessor,
                                        ComponentLog logger) {
        this.chunkId = chunkId;
        this.dataProcessor = dataProcessor;
        this.logger = logger;
    }

    public void process(InputStream data) {
        try {
            if (stopped.get()) {
                logger.warn("Gpfdist segment request processor is already stopped for greengageChunkId {}", chunkId);
                throw new IllegalStateException("Segment processor already stopped for greengageChunkId: " + chunkId);
            }
            dataProcessor.process(data);
        } catch (Exception e) {
            logger.error("Failed to process input data", e);
            stopExceptionally(e);
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        stopped.set(true);
        logger.info("Segment request processor for greengageChunkId {} is stopped", chunkId);
    }

    private void stopExceptionally(Throwable error) {
        stopped.set(true);
        getResult().setError(error);
    }

    public boolean getStopped() {
        return stopped.get();
    }

    public UnloadingResult getResult() {
        return dataProcessor.getResult();
    }

    public GreengageChunkId getChunkId() {
        return chunkId;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GpfdistChunkRequestProcessor that = (GpfdistChunkRequestProcessor) o;
        return Objects.equals(chunkId, that.chunkId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(chunkId);
    }

    @Override
    public String toString() {
        return "GpfdistSegmentRequestProcessor{" +
                "greengageChunkId=" + chunkId +
                ", stopped=" + stopped +
                '}';
    }
}
