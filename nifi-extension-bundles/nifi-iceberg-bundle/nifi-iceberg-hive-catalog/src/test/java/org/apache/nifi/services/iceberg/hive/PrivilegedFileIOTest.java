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
package org.apache.nifi.services.iceberg.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.DelegatingOutputStream;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class PrivilegedFileIOTest {
    private static final String LOCATION = "file:/warehouse/records/data/file.parquet";

    private static final String FAILED_LOCATION = "file:/warehouse/records/data/failed.parquet";

    private static final byte[] CONTENT = "records".getBytes(StandardCharsets.UTF_8);

    private static final int EXPECTED_OPERATIONS = 13;

    @TempDir
    private Path tempDirectory;

    private HadoopIdentity identity;

    @BeforeEach
    void setIdentity() throws IOException {
        identity = HadoopIdentity.login(new Configuration(false), null, mock(ComponentLog.class));
    }

    @AfterEach
    void closeIdentity() {
        identity.close();
    }

    @Test
    void testOperationsRunAsIdentity() {
        final RecordingFileIO recordingFileIO = new RecordingFileIO();
        final PrivilegedFileIO fileIO = new PrivilegedFileIO(recordingFileIO, identity);

        final InputFile inputFile = fileIO.newInputFile(LOCATION);
        inputFile.getLength();
        inputFile.newStream();
        inputFile.exists();
        fileIO.newInputFile(LOCATION, CONTENT.length);

        final OutputFile outputFile = fileIO.newOutputFile(LOCATION);
        outputFile.create();
        outputFile.createOrOverwrite();
        outputFile.toInputFile().exists();

        fileIO.deleteFile(LOCATION);
        fileIO.listPrefix(LOCATION);
        fileIO.deletePrefix(LOCATION);
        fileIO.deleteFiles(List.of(LOCATION));

        assertEquals(EXPECTED_OPERATIONS, recordingFileIO.users.size(), recordingFileIO.users::toString);
        assertTrue(recordingFileIO.users.stream().allMatch(identity.getUserGroupInformation()::equals), recordingFileIO.users::toString);
    }

    @Test
    void testDeleteFilesContinuesAfterFailure() {
        final RecordingFileIO recordingFileIO = new RecordingFileIO() {
            @Override
            public void deleteFile(final String path) {
                super.deleteFile(path);
                if (FAILED_LOCATION.equals(path)) {
                    throw new IllegalStateException(path);
                }
            }
        };
        final PrivilegedFileIO fileIO = new PrivilegedFileIO(recordingFileIO, identity);

        final BulkDeletionFailureException exception = assertThrows(BulkDeletionFailureException.class, () -> fileIO.deleteFiles(List.of(FAILED_LOCATION, LOCATION)));

        assertEquals(1, exception.numberFailedObjects());
        assertEquals(2, recordingFileIO.users.size());
        assertTrue(recordingFileIO.users.stream().allMatch(identity.getUserGroupInformation()::equals), recordingFileIO.users::toString);
    }

    // Parquet unwraps delegating Iceberg streams and drops the Iceberg Hadoop wrapper, whose finalizer closes the stream in use
    @Test
    void testStreamsNotUnwrappedToHadoopStreams() throws IOException {
        final PrivilegedFileIO fileIO = new PrivilegedFileIO(new HadoopFileIO(new Configuration()), identity);
        final String location = tempDirectory.resolve("records.bin").toUri().toString();

        try (PositionOutputStream outputStream = fileIO.newOutputFile(location).create()) {
            assertFalse(outputStream instanceof DelegatingOutputStream, outputStream.getClass()::getName);
            outputStream.write(CONTENT);
        }

        try (SeekableInputStream inputStream = fileIO.newInputFile(location).newStream()) {
            assertFalse(inputStream instanceof DelegatingInputStream, inputStream.getClass()::getName);
            inputStream.seek(1);
            assertEquals(1, inputStream.getPos());
            assertArrayEquals(Arrays.copyOfRange(CONTENT, 1, CONTENT.length), inputStream.readAllBytes());
        }
    }

    @Test
    void testHadoopFileIOWriteReadDelete() throws IOException {
        final PrivilegedFileIO fileIO = new PrivilegedFileIO(new HadoopFileIO(new Configuration()), identity);
        final String prefix = tempDirectory.toUri().toString();
        final String location = tempDirectory.resolve("records.bin").toUri().toString();

        try (PositionOutputStream outputStream = fileIO.newOutputFile(location).create()) {
            outputStream.write(CONTENT);
        }

        final InputFile inputFile = fileIO.newInputFile(location);
        assertTrue(inputFile.exists());
        assertEquals(CONTENT.length, inputFile.getLength());
        try (SeekableInputStream inputStream = inputFile.newStream()) {
            assertArrayEquals(CONTENT, inputStream.readAllBytes());
        }

        final List<FileInfo> files = new ArrayList<>();
        fileIO.listPrefix(prefix).forEach(files::add);
        assertEquals(1, files.size());

        fileIO.deleteFiles(List.of(location));
        assertFalse(fileIO.newInputFile(location).exists());
    }

    private static class RecordingFileIO implements DelegateFileIO {
        private final List<UserGroupInformation> users = new ArrayList<>();

        @Override
        public InputFile newInputFile(final String path) {
            record();
            return new RecordingInputFile(path, users);
        }

        @Override
        public InputFile newInputFile(final String path, final long length) {
            record();
            return new RecordingInputFile(path, users);
        }

        @Override
        public OutputFile newOutputFile(final String path) {
            record();
            return new RecordingOutputFile(path, users);
        }

        @Override
        public void deleteFile(final String path) {
            record();
        }

        @Override
        public Iterable<FileInfo> listPrefix(final String prefix) {
            record();
            return List.of();
        }

        @Override
        public void deletePrefix(final String prefix) {
            record();
        }

        @Override
        public void deleteFiles(final Iterable<String> paths) {
            record();
        }

        private void record() {
            users.add(getCurrentUser());
        }
    }

    private record RecordingInputFile(String location, List<UserGroupInformation> users) implements InputFile {
        @Override
        public long getLength() {
            users.add(getCurrentUser());
            return CONTENT.length;
        }

        @Override
        public SeekableInputStream newStream() {
            users.add(getCurrentUser());
            return null;
        }

        @Override
        public boolean exists() {
            users.add(getCurrentUser());
            return true;
        }
    }

    private record RecordingOutputFile(String location, List<UserGroupInformation> users) implements OutputFile {
        @Override
        public PositionOutputStream create() {
            users.add(getCurrentUser());
            return null;
        }

        @Override
        public PositionOutputStream createOrOverwrite() {
            users.add(getCurrentUser());
            return null;
        }

        @Override
        public InputFile toInputFile() {
            return new RecordingInputFile(location, users);
        }
    }

    private static UserGroupInformation getCurrentUser() {
        try {
            return UserGroupInformation.getCurrentUser();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
