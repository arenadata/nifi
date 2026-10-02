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

import org.apache.commons.lang3.ClassUtils;
import org.apache.iceberg.PendingUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Set;

/**
 * Runs Apache Iceberg Catalog operations with the Controller Service ClassLoader as the thread context ClassLoader. The
 * Hive Metastore client is created through the thread context ClassLoader, while Table and pending update objects come
 * from a ClassLoader shared with the Processor, so the framework returns them without its own proxy and a commit would
 * otherwise run with a ClassLoader that cannot see the Hive Metastore client. Objects returned from an invocation are
 * proxied so that operations reached through the Catalog keep the same context.
 */
class ContextClassLoaderInvocationHandler implements InvocationHandler {
    private static final Set<Class<?>> PROXIED_TYPES = Set.of(
            Catalog.class,
            Table.class,
            FileIO.class,
            InputFile.class,
            OutputFile.class,
            PendingUpdate.class
    );

    private final Object delegate;

    private final ClassLoader classLoader;

    private ContextClassLoaderInvocationHandler(final Object delegate, final ClassLoader classLoader) {
        this.delegate = delegate;
        this.classLoader = classLoader;
    }

    static <T> T getProxy(final Class<T> type, final T delegate, final ClassLoader classLoader) {
        return type.cast(getProxy(delegate, classLoader));
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        if (Object.class.equals(method.getDeclaringClass())) {
            return invokeMethod(method, args);
        }

        final Thread thread = Thread.currentThread();
        final ClassLoader callerClassLoader = thread.getContextClassLoader();
        thread.setContextClassLoader(classLoader);
        try {
            return getProxiedResult(invokeMethod(method, args));
        } finally {
            thread.setContextClassLoader(callerClassLoader);
        }
    }

    private Object invokeMethod(final Method method, final Object[] args) throws Throwable {
        try {
            return method.invoke(delegate, args);
        } catch (final InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private Object getProxiedResult(final Object result) {
        if (result == null) {
            return null;
        }

        final boolean proxied = PROXIED_TYPES.stream().anyMatch(type -> type.isInstance(result));
        return proxied ? getProxy(result, classLoader) : result;
    }

    private static Object getProxy(final Object delegate, final ClassLoader classLoader) {
        final List<Class<?>> interfaces = ClassUtils.getAllInterfaces(delegate.getClass());
        final InvocationHandler handler = new ContextClassLoaderInvocationHandler(delegate, classLoader);
        return Proxy.newProxyInstance(delegate.getClass().getClassLoader(), interfaces.toArray(new Class<?>[0]), handler);
    }
}
