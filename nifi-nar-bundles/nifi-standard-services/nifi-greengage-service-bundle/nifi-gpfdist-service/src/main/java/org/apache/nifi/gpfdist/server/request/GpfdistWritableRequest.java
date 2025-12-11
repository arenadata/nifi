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
package org.apache.nifi.gpfdist.server.request;

import org.apache.nifi.gpfdist.service.unload.dto.GreengageChunkId;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_CID;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_DATABASE;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_DONE;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_LINE_DELIM_LENGTH;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_PROTO;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_PROTOCOL_VERSION;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_SEGMENT_COUNT;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_SEGMENT_ID;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_SEQUENCE;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_SN;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_XID;

public class GpfdistWritableRequest
{
    private String requestId;
    private String transactionId;
    private String commandId;
    private String scanId;
    private Integer segmentId;
    private Optional<Integer> segmentsCount;
    private Optional<Integer> lineDelimiterLength;
    private short gpProtocol;
    private Optional<String> gpProtocolVersion;
    private int gpSequence;
    private Optional<Boolean> isLastChunk;
    private Optional<String> gpDatabase;

    public GpfdistWritableRequest(
            String requestId,
            String transactionId,
            String commandId,
            String scanId,
            Integer segmentId,
            Optional<Integer> segmentsCount,
            Optional<Integer> lineDelimiterLength,
            short gpProtocol,
            Optional<String> gpProtocolVersion,
            int gpSequence,
            Optional<Boolean> isLastChunk,
            Optional<String> gpDatabase)
    {
        this.requestId = requestId;
        this.transactionId = transactionId;
        this.commandId = commandId;
        this.scanId = scanId;
        this.segmentId = segmentId;
        this.segmentsCount = segmentsCount;
        this.lineDelimiterLength = lineDelimiterLength;
        this.gpProtocol = gpProtocol;
        this.gpProtocolVersion = gpProtocolVersion;
        this.gpSequence = gpSequence;
        this.isLastChunk = isLastChunk;
        this.gpDatabase = gpDatabase;
    }

    public static GpfdistWritableRequest create(String tableName, Map<String, String> values)
    {
        return new GpfdistWritableRequest(
                createRequestId(tableName),
                values.get(X_GP_XID),
                values.get(X_GP_CID),
                values.get(X_GP_SN),
                Optional.ofNullable(values.get(X_GP_SEGMENT_ID))
                        .map(Integer::parseInt)
                        .orElseThrow(
                                () -> new IllegalArgumentException("Request header not found: " + X_GP_SEGMENT_ID)),
                Optional.ofNullable(values.get(X_GP_SEGMENT_COUNT))
                        .map(Integer::parseInt),
                Optional.ofNullable(values.get(X_GP_LINE_DELIM_LENGTH))
                        .map(Integer::parseInt),
                Short.parseShort(values.get(X_GP_PROTO)),
                Optional.ofNullable(values.get(X_GP_PROTOCOL_VERSION)),
                Integer.parseInt(values.get(X_GP_SEQUENCE)),
                Optional.ofNullable(values.get(X_GP_DONE))
                        .map(v -> v.equals("1")),
                Optional.ofNullable(values.get(X_GP_DATABASE)));
    }

    private static String createRequestId(String tableName)
    {
        return tableName + "_" + UUID.randomUUID();
    }

    public short getGpProtocol()
    {
        return gpProtocol;
    }

    public int getGpSequence()
    {
        return gpSequence;
    }

    public Optional<Boolean> isLastChunk()
    {
        return isLastChunk;
    }

    public GreengageChunkId getChunkId() {
        return new GreengageChunkId(transactionId, segmentId);
    }

    @Override
    public String toString()
    {
        return "GpfdistWritableRequest{" +
                "requestId='" + requestId + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", commandId='" + commandId + '\'' +
                ", scanId='" + scanId + '\'' +
                ", segmentId=" + segmentId +
                ", segmentsCount=" + segmentsCount +
                ", lineDelimiterLength=" + lineDelimiterLength +
                ", gpProtocol=" + gpProtocol +
                ", gpProtocolVersion=" + gpProtocolVersion +
                ", gpSequence=" + gpSequence +
                ", isLastChunk=" + isLastChunk +
                ", gpDatabase=" + gpDatabase +
                '}';
    }
}
