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
package org.apache.nifi.gpfdist.metadata;

import java.util.Objects;

public class GpfidstLoadConfig {
    private final long asyncContextTimeoutMs;
    private final long gpfdistSegmentStreamBufferSize;
    private final long gpfdistStreamBufferEnqueueTimeoutMs;

    public GpfidstLoadConfig(long asyncContextTimeoutMs,
                             long gpfdistSegmentStreamBufferSize,
                             long gpfdistStreamBufferEnqueueTimeoutMs) {
        this.asyncContextTimeoutMs = asyncContextTimeoutMs;
        this.gpfdistSegmentStreamBufferSize = gpfdistSegmentStreamBufferSize;
        this.gpfdistStreamBufferEnqueueTimeoutMs = gpfdistStreamBufferEnqueueTimeoutMs;
    }

    public long getAsyncContextTimeoutMs() {
        return asyncContextTimeoutMs;
    }

    public long getGpfdistSegmentStreamBufferSize() {
        return gpfdistSegmentStreamBufferSize;
    }

    public long getGpfdistStreamBufferEnqueueTimeoutMs() {
        return gpfdistStreamBufferEnqueueTimeoutMs;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GpfidstLoadConfig that = (GpfidstLoadConfig) o;
        return asyncContextTimeoutMs == that.asyncContextTimeoutMs
                && gpfdistSegmentStreamBufferSize == that.gpfdistSegmentStreamBufferSize
                && gpfdistStreamBufferEnqueueTimeoutMs == that.gpfdistStreamBufferEnqueueTimeoutMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(asyncContextTimeoutMs, gpfdistSegmentStreamBufferSize, gpfdistStreamBufferEnqueueTimeoutMs);
    }

    @Override
    public String toString() {
        return "GpfidstLoadConfig{" +
                "asyncContextTimeoutMs=" + asyncContextTimeoutMs +
                ", gpfdistSegmentStreamBufferSize=" + gpfdistSegmentStreamBufferSize +
                ", gpfdistStreamBufferEnqueueTimeoutMs=" + gpfdistStreamBufferEnqueueTimeoutMs +
                '}';
    }
}
