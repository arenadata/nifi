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

import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * File IO running file operations as the Hadoop Identity of the Controller Service. Input and Output Files are wrapped
 * as well, because Parquet and other format writers resolve the Hadoop File System again for Hadoop file types using
 * the current user of the calling thread, which is a Processor or an Iceberg worker thread outside the identity.
 * Streams run without the identity: they belong to a File System client created as the identity, which authenticates
 * new connections with the identity Subject. Streams are wrapped only to hide the Hadoop stream inside: Parquet unwraps
 * Iceberg Hadoop streams and drops the Iceberg wrapper, whose finalizer then closes the Hadoop stream while it is in use.
 */
class PrivilegedFileIO implements DelegateFileIO {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrivilegedFileIO.class);

    private final DelegateFileIO delegate;

    private final transient HadoopIdentity identity;

    PrivilegedFileIO(final DelegateFileIO delegate, final HadoopIdentity identity) {
        this.delegate = delegate;
        this.identity = identity;
    }

    @Override
    public InputFile newInputFile(final String path) {
        return new PrivilegedInputFile(identity.doAs(() -> delegate.newInputFile(path)), identity);
    }

    @Override
    public InputFile newInputFile(final String path, final long length) {
        return new PrivilegedInputFile(identity.doAs(() -> delegate.newInputFile(path, length)), identity);
    }

    @Override
    public OutputFile newOutputFile(final String path) {
        return new PrivilegedOutputFile(identity.doAs(() -> delegate.newOutputFile(path)), identity);
    }

    @Override
    public void deleteFile(final String path) {
        identity.doAs(() -> {
            delegate.deleteFile(path);
            return null;
        });
    }

    @Override
    public Iterable<FileInfo> listPrefix(final String prefix) {
        return identity.doAs(() -> delegate.listPrefix(prefix));
    }

    @Override
    public void deletePrefix(final String prefix) {
        identity.doAs(() -> {
            delegate.deletePrefix(prefix);
            return null;
        });
    }

    // Hadoop File IO runs bulk deletes on a shared static thread pool outside the identity, so files are deleted one by one
    @Override
    public void deleteFiles(final Iterable<String> paths) {
        final int failed = identity.doAs(() -> {
            int failures = 0;
            for (final String path : paths) {
                try {
                    delegate.deleteFile(path);
                } catch (final RuntimeException e) {
                    LOGGER.warn("Delete File failed [{}]", path, e);
                    failures++;
                }
            }
            return failures;
        });

        if (failed > 0) {
            throw new BulkDeletionFailureException(failed);
        }
    }

    @Override
    public Map<String, String> properties() {
        return delegate.properties();
    }

    @Override
    public void initialize(final Map<String, String> properties) {
        delegate.initialize(properties);
    }

    @Override
    public void close() {
        delegate.close();
    }

    DelegateFileIO getDelegate() {
        return delegate;
    }

    private record PrivilegedInputFile(InputFile delegate, HadoopIdentity identity) implements InputFile {
        @Override
        public long getLength() {
            return identity.doAs(delegate::getLength);
        }

        @Override
        public SeekableInputStream newStream() {
            return new RetainedSeekableInputStream(identity.doAs(delegate::newStream));
        }

        @Override
        public String location() {
            return delegate.location();
        }

        @Override
        public boolean exists() {
            return identity.doAs(delegate::exists);
        }
    }

    private record PrivilegedOutputFile(OutputFile delegate, HadoopIdentity identity) implements OutputFile {
        @Override
        public PositionOutputStream create() {
            return new RetainedPositionOutputStream(identity.doAs(delegate::create));
        }

        @Override
        public PositionOutputStream createOrOverwrite() {
            return new RetainedPositionOutputStream(identity.doAs(delegate::createOrOverwrite));
        }

        @Override
        public String location() {
            return delegate.location();
        }

        @Override
        public InputFile toInputFile() {
            return new PrivilegedInputFile(delegate.toInputFile(), identity);
        }
    }

    private static final class RetainedSeekableInputStream extends SeekableInputStream {
        private final SeekableInputStream delegate;

        private RetainedSeekableInputStream(final SeekableInputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getPos() throws IOException {
            return delegate.getPos();
        }

        @Override
        public void seek(final long position) throws IOException {
            delegate.seek(position);
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }

        @Override
        public int read(final byte[] buffer, final int offset, final int length) throws IOException {
            return delegate.read(buffer, offset, length);
        }

        @Override
        public long skip(final long length) throws IOException {
            return delegate.skip(length);
        }

        @Override
        public int available() throws IOException {
            return delegate.available();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class RetainedPositionOutputStream extends PositionOutputStream {
        private final PositionOutputStream delegate;

        private RetainedPositionOutputStream(final PositionOutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getPos() throws IOException {
            return delegate.getPos();
        }

        @Override
        public long storedLength() throws IOException {
            return delegate.storedLength();
        }

        @Override
        public void write(final int value) throws IOException {
            delegate.write(value);
        }

        @Override
        public void write(final byte[] buffer, final int offset, final int length) throws IOException {
            delegate.write(buffer, offset, length);
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
