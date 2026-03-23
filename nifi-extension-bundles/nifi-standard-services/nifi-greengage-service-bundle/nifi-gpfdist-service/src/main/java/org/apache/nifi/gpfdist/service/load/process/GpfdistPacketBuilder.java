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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.gpfdist.service.load.serialization.RecordsSerializationResult;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Gpfdist v1 packet structure:
 * byte 0: type (can be 'F'ilename, 'O'ffset, 'D'ata, 'E'rror, 'L'inenumber)
 * byte 1-4: length. # bytes of following data block. in network-order.
 * byte 5-X: the block itself
 * Data packets:
 * F + fileName length + fileName + O + offset length + offset + L + lineNumber length + lineNumber + D + data length + data
 * End packet:
 * D + zero length
 * Error packet:
 * E + error data length + error data
 */
public class GpfdistPacketBuilder {
    private static final byte GPFDIST_PACKAGE_FILE_NAME_MSG_TYPE = (byte) 70;
    private static final byte GPFDIST_PACKAGE_OFFSET_MSG_TYPE = (byte) 79;
    private static final byte GPFDIST_PACKAGE_LINE_NUMBER_MSG_TYPE = (byte) 76;
    private static final byte GPFDIST_PACKAGE_END_MSG_TYPE = (byte) 68;
    private static final byte GPFDIST_PACKAGE_ERROR_MSG_TYPE = (byte) 69;
    private static final int HEADER_METADATA_TYPE_BYTES_LENGTH = 5;
    private static final int OFFSET_BYTES_LENGTH = 8;
    private static final int LINE_NUMBER_BYTES_LENGTH = 8;
    private final String gpfdistFileName;
    private final ByteBuffer headerBuffer;
    private final ByteBuffer endPacketBuffer;

    public GpfdistPacketBuilder(final String gpfdistFileName) {
        this.gpfdistFileName = gpfdistFileName;
        headerBuffer = ByteBuffer.allocate(HEADER_METADATA_TYPE_BYTES_LENGTH * 4
                + OFFSET_BYTES_LENGTH
                + LINE_NUMBER_BYTES_LENGTH
                + gpfdistFileName.getBytes(StandardCharsets.UTF_8).length);
        endPacketBuffer = ByteBuffer.allocate(HEADER_METADATA_TYPE_BYTES_LENGTH);
    }

    public byte[] createDataPacket(RecordsSerializationResult serializationResult) {
        headerBuffer.clear();
        byte[] fileNameBytes = gpfdistFileName.getBytes(StandardCharsets.UTF_8);
        headerBuffer.put(GPFDIST_PACKAGE_FILE_NAME_MSG_TYPE);
        headerBuffer.putInt(fileNameBytes.length);
        headerBuffer.put(fileNameBytes);
        headerBuffer.put(GPFDIST_PACKAGE_OFFSET_MSG_TYPE);
        headerBuffer.putInt(OFFSET_BYTES_LENGTH);
        headerBuffer.putLong(0);
        headerBuffer.put(GPFDIST_PACKAGE_LINE_NUMBER_MSG_TYPE);
        headerBuffer.putInt(LINE_NUMBER_BYTES_LENGTH);
        headerBuffer.putLong(serializationResult.getRowCount());
        headerBuffer.put(GPFDIST_PACKAGE_END_MSG_TYPE);
        headerBuffer.putInt(serializationResult.getData().length);
        return ArrayUtils.addAll(headerBuffer.array(), serializationResult.getData());
    }

    public byte[] createSingleEmptyDataPacket() {
        return createDataPacket(new RecordsSerializationResult(new byte[]{}, 0));
    }

    public byte[] createEndPacket() {
        endPacketBuffer.clear();
        endPacketBuffer.put(GPFDIST_PACKAGE_END_MSG_TYPE);
        endPacketBuffer.putInt(0);
        return endPacketBuffer.array();
    }

    public byte[] createErrorPacket(Throwable error) {
        byte[] msgBytes = error.getMessage().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_METADATA_TYPE_BYTES_LENGTH + msgBytes.length);
        buffer.put(GPFDIST_PACKAGE_ERROR_MSG_TYPE);
        buffer.putInt(msgBytes.length);
        buffer.put(msgBytes);
        return buffer.array();
    }
}
