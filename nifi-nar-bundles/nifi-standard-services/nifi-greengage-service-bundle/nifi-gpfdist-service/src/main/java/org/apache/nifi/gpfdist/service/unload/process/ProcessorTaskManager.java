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

import org.apache.nifi.logging.ComponentLog;

import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ProcessorTaskManager {
    private final int globalParallelFactor;
    private final ComponentLog logger;
    private final ConcurrentLinkedQueue<ProcessorTask> processorTasksQueue;

    public ProcessorTaskManager(int nodeIndex,
                                int nodeCount,
                                int nodeParallelFactor,
                                int ggSegmentCount,
                                ComponentLog logger) {
        processorTasksQueue = new ConcurrentLinkedQueue<>();
        globalParallelFactor = Math.min(nodeCount * nodeParallelFactor, ggSegmentCount);
        for (int workerIndex = 0; workerIndex < globalParallelFactor; workerIndex++) {
            if (workerIndex % nodeCount == nodeIndex) {
                int taskSlotId = processorTasksQueue.size();
                processorTasksQueue.add(new ProcessorTask(
                        UUID.randomUUID().toString(),
                        taskSlotId,
                        workerIndex));
                logger.info("Added task {} to queue");
            }
        }
        if (nodeCount * nodeParallelFactor > ggSegmentCount) {
            logger.warn("Configured nodeParallelFactor {} with nodeCount {} results in {} tasks, " +
                            "but gpSegmentCount is {}. Global parallel factor capped to {}.",
                    nodeParallelFactor,
                    nodeCount,
                    nodeCount * nodeParallelFactor,
                    ggSegmentCount,
                    globalParallelFactor);
        }
        logger.info("Initialized task slot manager: nodeIndex={}, nodeCount={}, nodeParallelFactor={}, globalParallelFactor={}, ggSegmentCount={}, taskSlotsSize={}",
                nodeIndex, nodeCount, nodeParallelFactor, globalParallelFactor, ggSegmentCount, processorTasksQueue.size());
        this.logger = logger;
    }

    public ProcessorTask acquire() {
        return processorTasksQueue.poll();
    }

    public void release(ProcessorTask task) {
        if (task != null) {
            processorTasksQueue.add(task);
            logger.info("Released task slot: {}", task);
        }
    }

    public void remove(ProcessorTask task) {
        processorTasksQueue.remove(task);
        logger.info("Removed task slot: {}", task);
    }

    public ConcurrentLinkedQueue<ProcessorTask> getProcessorTask() {
        return processorTasksQueue;
    }

    public int getGlobalParallelFactor() {
        return globalParallelFactor;
    }

    public static final class ProcessorTask {
        private final String id;
        private final int slotId;
        private final int globalWorkerIndex;

        public ProcessorTask(String id, int slotId, int globalWorkerIndex) {
            this.id = id;
            this.slotId = slotId;
            this.globalWorkerIndex = globalWorkerIndex;
        }

        public int getSlotId() {
            return slotId;
        }

        public String getId() {
            return id;
        }

        public int getGlobalWorkerIndex() {
            return globalWorkerIndex;
        }

        @Override
        public String toString() {
            return "ProcessorTask{" +
                    "id='" + id + '\'' +
                    ", slotId=" + slotId +
                    ", globalWorkerIndex=" + globalWorkerIndex +
                    '}';
        }
    }
}
