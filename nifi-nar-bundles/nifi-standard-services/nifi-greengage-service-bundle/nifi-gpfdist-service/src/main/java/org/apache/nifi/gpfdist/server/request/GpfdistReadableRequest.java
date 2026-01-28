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
package org.apache.nifi.gpfdist.server.request;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_CID;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_CSV_OPT;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_DATABASE;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_LINE_DELIM_LENGTH;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_MASTER_HOST;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_MASTER_PORT;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_PROTO;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_SEGMENT_COUNT;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_SEGMENT_ID;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_SEG_DATADIR;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_SEG_PG_CONF;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_SESSION_ID;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_SN;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_XID;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_X_GP_USER;
import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_X_SEG_PORT;

public class GpfdistReadableRequest extends ReadableRequest {
    private final String transactionId;
    private final String commandId;
    private final String scanId;
    private final int segmentId;
    private final int segmentsCount;
    private final Optional<Integer> lineDelimiterLength;
    private final short gpProtocol;
    private final Optional<String> gpMasterHost;
    private final Optional<Integer> gpMasterPort;
    private final Optional<String> gpcsvFormat;
    private final Optional<String> gpSegmentConfigPath;
    private final Optional<String> gpSegmentDataDirectory;
    private final Optional<String> gpDatabase;
    private final Optional<String> gpUser;
    private final Optional<Integer> gpSegmentPort;
    private final Optional<Integer> gpSessionId;

    public GpfdistReadableRequest(
            String requestId,
            String transactionId,
            String commandId,
            String scanId,
            int segmentId,
            int segmentsCount,
            Optional<Integer> lineDelimiterLength,
            short gpProtocol,
            Optional<String> gpMasterHost,
            Optional<Integer> gpMasterPort,
            Optional<String> gpcsvFormat,
            Optional<String> gpSegmentConfigPath,
            Optional<String> gpSegmentDataDirectory,
            Optional<String> gpDatabase,
            Optional<String> gpUser,
            Optional<Integer> gpSegmentPort,
            Optional<Integer> gpSessionId) {
        super(requestId);
        this.transactionId = transactionId;
        this.commandId = commandId;
        this.scanId = scanId;
        this.segmentId = segmentId;
        this.segmentsCount = segmentsCount;
        this.lineDelimiterLength = lineDelimiterLength;
        this.gpProtocol = gpProtocol;
        this.gpMasterHost = gpMasterHost;
        this.gpMasterPort = gpMasterPort;
        this.gpcsvFormat = gpcsvFormat;
        this.gpSegmentConfigPath = gpSegmentConfigPath;
        this.gpSegmentDataDirectory = gpSegmentDataDirectory;
        this.gpDatabase = gpDatabase;
        this.gpUser = gpUser;
        this.gpSegmentPort = gpSegmentPort;
        this.gpSessionId = gpSessionId;
    }

    public static GpfdistReadableRequest create(String tableName, Map<String, String> values) {
        return new GpfdistReadableRequest(
                createRequestId(tableName),
                values.get(X_GP_XID),
                values.get(X_GP_CID),
                values.get(X_GP_SN),
                Optional.ofNullable(values.get(X_GP_SEGMENT_ID))
                        .map(Integer::parseInt)
                        .orElseThrow(() -> new IllegalArgumentException("Failed to get segmentId from gpfdist request header")),
                Optional.ofNullable(values.get(X_GP_SEGMENT_COUNT))
                        .map(Integer::parseInt)
                        .orElseThrow(() -> new IllegalArgumentException("Failed to get segmentCount from gpfdist request header")),
                Optional.ofNullable(values.get(X_GP_LINE_DELIM_LENGTH))
                        .map(Integer::parseInt),
                Short.parseShort(values.get(X_GP_PROTO)),
                Optional.ofNullable(values.get(X_GP_MASTER_HOST)),
                Optional.ofNullable(values.get(X_GP_MASTER_PORT))
                        .map(Integer::parseInt),
                Optional.ofNullable(values.get(X_GP_CSV_OPT)),
                Optional.ofNullable(values.get(X_GP_SEG_PG_CONF)),
                Optional.ofNullable(values.get(X_GP_SEG_DATADIR)),
                Optional.ofNullable(values.get(X_GP_DATABASE)),
                Optional.ofNullable(values.get(X_GP_X_GP_USER)),
                Optional.ofNullable(values.get(X_GP_X_SEG_PORT))
                        .map(Integer::parseInt),
                Optional.ofNullable(values.get(X_GP_SESSION_ID))
                        .map(Integer::parseInt));
    }

    private static String createRequestId(String tableName) {
        return tableName + "_" + UUID.randomUUID();
    }

    public short getGpProtocol() {
        return gpProtocol;
    }

    public int getSegmentId() {
        return segmentId;
    }

    public int getSegmentsCount() {
        return segmentsCount;
    }

    @Override
    public String toString() {
        return "GpfdistReadableRequest{" +
                "requestId='" + getRequestId() + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", commandId='" + commandId + '\'' +
                ", scanId='" + scanId + '\'' +
                ", segmentId=" + segmentId +
                ", segmentsCount=" + segmentsCount +
                ", lineDelimiterLength=" + lineDelimiterLength +
                ", gpProtocol=" + gpProtocol +
                ", gpMasterHost=" + gpMasterHost +
                ", gpMasterPort=" + gpMasterPort +
                ", gpcsvFormat=" + gpcsvFormat +
                ", gpSegmentConfigPath=" + gpSegmentConfigPath +
                ", gpSegmentDataDirectory=" + gpSegmentDataDirectory +
                ", gpDatabase=" + gpDatabase +
                ", gpUser=" + gpUser +
                ", gpSegmentPort=" + gpSegmentPort +
                ", gpSessionId=" + gpSessionId +
                '}';
    }
}
