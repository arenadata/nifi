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

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * Verifies that Catalog operations and the Table, File IO and pending update operations reached through the Catalog
 * run with the Controller Service ClassLoader as the thread context ClassLoader, and that the caller ClassLoader is
 * restored afterwards.
 */
@ExtendWith(MockitoExtension.class)
class ContextClassLoaderInvocationHandlerTest {
    private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(Namespace.of("db"), "records");

    private static final String LOCATION = "hdfs://cluster/warehouse/db/records/data/file.parquet";

    @Mock
    private Catalog catalog;

    @Mock
    private Table table;

    @Mock
    private AppendFiles appendFiles;

    @Mock
    private FileIO fileIO;

    @Mock
    private OutputFile outputFile;

    private final AtomicReference<ClassLoader> invocationClassLoader = new AtomicReference<>();

    private URLClassLoader serviceClassLoader;

    private URLClassLoader callerClassLoader;

    private ClassLoader originalClassLoader;

    private Catalog proxiedCatalog;

    @BeforeEach
    void setClassLoaders() {
        originalClassLoader = Thread.currentThread().getContextClassLoader();
        serviceClassLoader = new URLClassLoader(new URL[0], getClass().getClassLoader());
        callerClassLoader = new URLClassLoader(new URL[0], getClass().getClassLoader());
        Thread.currentThread().setContextClassLoader(callerClassLoader);

        proxiedCatalog = ContextClassLoaderInvocationHandler.getProxy(Catalog.class, catalog, serviceClassLoader);
    }

    @AfterEach
    void restoreClassLoader() throws IOException {
        Thread.currentThread().setContextClassLoader(originalClassLoader);
        serviceClassLoader.close();
        callerClassLoader.close();
    }

    @Test
    void testCatalogInvocationUsesServiceClassLoader() {
        when(catalog.loadTable(TABLE_IDENTIFIER)).thenAnswer(invocation -> recordClassLoader(table));

        proxiedCatalog.loadTable(TABLE_IDENTIFIER);

        assertSame(serviceClassLoader, invocationClassLoader.get());
        assertSame(callerClassLoader, Thread.currentThread().getContextClassLoader());
    }

    @Test
    void testAppendCommitUsesServiceClassLoader() {
        when(catalog.loadTable(TABLE_IDENTIFIER)).thenReturn(table);
        when(table.newAppend()).thenReturn(appendFiles);
        doAnswer(invocation -> recordClassLoader(null)).when(appendFiles).commit();

        final AppendFiles proxiedAppend = proxiedCatalog.loadTable(TABLE_IDENTIFIER).newAppend();
        assertTrue(Proxy.isProxyClass(proxiedAppend.getClass()));

        proxiedAppend.commit();

        assertSame(serviceClassLoader, invocationClassLoader.get());
        assertSame(callerClassLoader, Thread.currentThread().getContextClassLoader());
    }

    @Test
    void testOutputFileCreationUsesServiceClassLoader() {
        when(catalog.loadTable(TABLE_IDENTIFIER)).thenReturn(table);
        when(table.io()).thenReturn(fileIO);
        when(fileIO.newOutputFile(LOCATION)).thenReturn(outputFile);
        when(outputFile.create()).thenAnswer(invocation -> recordClassLoader(null));

        final OutputFile proxiedOutputFile = proxiedCatalog.loadTable(TABLE_IDENTIFIER).io().newOutputFile(LOCATION);
        assertTrue(Proxy.isProxyClass(proxiedOutputFile.getClass()));

        proxiedOutputFile.create();

        assertSame(serviceClassLoader, invocationClassLoader.get());
    }

    @Test
    void testExceptionPassedThroughAndClassLoaderRestored() {
        when(catalog.loadTable(TABLE_IDENTIFIER)).thenThrow(new NoSuchTableException("Table not found"));

        assertThrows(NoSuchTableException.class, () -> proxiedCatalog.loadTable(TABLE_IDENTIFIER));

        assertSame(callerClassLoader, Thread.currentThread().getContextClassLoader());
    }

    private <T> T recordClassLoader(final T result) {
        invocationClassLoader.set(Thread.currentThread().getContextClassLoader());
        return result;
    }
}
