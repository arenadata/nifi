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
package org.apache.nifi.services.iceberg.hive.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

/**
 * File and wait helpers shared by cluster components
 */
public final class ClusterFiles {
    private static final String CONFIGURATION_FORMAT = """
            <?xml version="1.0" encoding="UTF-8"?>
            <configuration>
            %s</configuration>
            """;

    private static final String PROPERTY_FORMAT = "    <property><name>%s</name><value>%s</value></property>%n";

    private ClusterFiles() {
    }

    /**
     * Render Hadoop XML configuration with escaped values
     *
     * @param properties Configuration properties
     * @return Hadoop XML configuration
     */
    public static String hadoopXml(final Map<String, String> properties) {
        final StringBuilder content = new StringBuilder();
        properties.forEach((name, value) -> content.append(PROPERTY_FORMAT.formatted(escape(name), escape(value))));
        return CONFIGURATION_FORMAT.formatted(content);
    }

    public static Path write(final Path path, final String content) {
        try {
            Files.createDirectories(path.getParent());
            Files.writeString(path, content);
            return path;
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Read classpath resource under cluster resources and replace ${name} placeholders
     *
     * @param resource Resource path relative to the cluster directory, such as ozone/start.sh
     * @param variables Placeholder values
     * @return Resource content with placeholders replaced
     */
    public static String resource(final String resource, final Map<String, String> variables) {
        final String path = "/cluster/%s".formatted(resource);
        try (InputStream inputStream = ClusterFiles.class.getResourceAsStream(path)) {
            if (inputStream == null) {
                throw new IllegalStateException("Resource not found [%s]".formatted(path));
            }
            String content = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            for (final Map.Entry<String, String> variable : variables.entrySet()) {
                content = content.replace("${%s}".formatted(variable.getKey()), variable.getValue());
            }
            return content;
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Poll a check until it succeeds or the timeout expires
     *
     * @param description Description used in the failure message
     * @param timeout Maximum time to wait
     * @param check Check that throws while the condition is not met
     * @throws InterruptedException Thrown when interrupted while waiting
     */
    public static void waitUntil(final String description, final Duration timeout, final Check check) throws InterruptedException {
        final long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            try {
                check.run();
                return;
            } catch (final Exception e) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("%s not available after %s".formatted(description, timeout), e);
                }
                Thread.sleep(ClusterContext.POLL_INTERVAL.toMillis());
            }
        }
    }

    private static String escape(final String value) {
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    @FunctionalInterface
    public interface Check {
        void run() throws Exception;
    }
}
