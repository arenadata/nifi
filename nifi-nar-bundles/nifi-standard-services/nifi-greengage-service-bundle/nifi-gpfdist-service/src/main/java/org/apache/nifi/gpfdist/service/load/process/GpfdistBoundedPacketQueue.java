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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class GpfdistBoundedPacketQueue {
    private final long maxBytes;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();
    private final ArrayDeque<byte[]> queue = new ArrayDeque<>();
    private volatile boolean closed = false;
    private long bufferedBytes;

    public GpfdistBoundedPacketQueue(long maxBytes) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("MaxBytes must be > 0");
        }
        this.maxBytes = maxBytes;
    }

    public boolean offer(byte[] packet, long timeoutMs) throws InterruptedException {
        if (packet == null) {
            throw new IllegalArgumentException("Packet cannot be null");
        }
        final long packetSize = packet.length;
        long nanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        lock.lock();
        try {
            while (!closed && (bufferedBytes + packetSize) > maxBytes) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
            }
            if (closed) {
                return false;
            }
            queue.addLast(packet);
            bufferedBytes += packetSize;
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    public byte[] poll() {
        lock.lock();
        try {
            byte[] polledBytes = queue.pollFirst();
            if (polledBytes != null) {
                bufferedBytes -= polledBytes.length;
                notFull.signal();
            }
            return polledBytes;
        } finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        lock.lock();
        try {
            return queue.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        lock.lock();
        try {
            closed = true;
            notEmpty.signalAll();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public long getBytesBuffered() {
        lock.lock();
        try {
            return bufferedBytes;
        } finally {
            lock.unlock();
        }
    }
}
