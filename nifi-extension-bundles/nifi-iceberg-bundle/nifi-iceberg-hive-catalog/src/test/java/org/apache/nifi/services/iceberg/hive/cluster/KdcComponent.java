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

import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * MIT Kerberos Key Distribution Center with hostname localhost, owning the network namespace and the published ports of
 * all cluster containers. Keytabs are exported with kadmin.local and copied into the host keytab directory.
 */
public final class KdcComponent implements Startable {
    private static final String IMAGE_NAME = "nifi-iceberg-kdc:1";

    private static final String RESOURCE_DIRECTORY_FORMAT = "cluster/kdc/%s";

    private static final String DOCKERFILE = "Dockerfile";

    private static final String ENTRYPOINT_SCRIPT = "entrypoint.sh";

    private static final String CREATE_KEYTAB_SCRIPT = "create-keytab.sh";

    private static final String SHELL = "/bin/bash";

    private static final String CREATE_KEYTAB_COMMAND = "/usr/local/bin/create-keytab.sh";

    private static final String CONTAINER_KEYTAB_FORMAT = "/var/lib/krb5kdc/keytabs/%s";

    private static final String REALM_VARIABLE = "KRB5_REALM";

    private static final String PORT_VARIABLE = "KDC_PORT";

    private static final String READY_PATTERN = ".*Key Distribution Center ready.*";

    private static final String POSIX_VIEW = "posix";

    private static final String KEYTAB_PERMISSIONS = "rw-r--r--";

    private static final String LOG_PREFIX = "kdc";

    private final ClusterContext context;

    private final GenericContainer<?> container;

    public KdcComponent(final ClusterContext context) {
        this.context = context;
        final ImageFromDockerfile image = new ImageFromDockerfile(IMAGE_NAME, false)
                .withFileFromClasspath(DOCKERFILE, RESOURCE_DIRECTORY_FORMAT.formatted(DOCKERFILE))
                .withFileFromClasspath(ENTRYPOINT_SCRIPT, RESOURCE_DIRECTORY_FORMAT.formatted(ENTRYPOINT_SCRIPT))
                .withFileFromClasspath(CREATE_KEYTAB_SCRIPT, RESOURCE_DIRECTORY_FORMAT.formatted(CREATE_KEYTAB_SCRIPT));
        this.container = new GenericContainer<>(image)
                .withEnv(REALM_VARIABLE, ClusterContext.REALM)
                .withEnv(PORT_VARIABLE, Integer.toString(ClusterContext.KDC_PORT))
                .withCopyToContainer(Transferable.of(context.krb5Conf()), ClusterContext.KRB5_CONF_PATH)
                .withCreateContainerCmdModifier(command -> command.withHostName(ClusterContext.HOST))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(KdcComponent.class)).withPrefix(LOG_PREFIX))
                .waitingFor(Wait.forLogMessage(READY_PATTERN, 1).withStartupTimeout(ClusterContext.STARTUP_TIMEOUT));
    }

    /**
     * Container owning the network namespace, available for port bindings before start
     *
     * @return Key Distribution Center container
     */
    public GenericContainer<?> container() {
        return container;
    }

    /**
     * Start the container and register it as the network namespace owner
     */
    @Override
    public void start() {
        container.start();
        context.setNetworkOwnerId(container.getContainerId());
    }

    @Override
    public void stop() {
        container.stop();
    }

    /**
     * Create principals with random keys when missing and export them into a keytab in the host keytab directory with
     * mode 0644. Keys of existing principals are exported without randomization, so a principal shared by several
     * keytabs keeps one valid key version in all of them.
     *
     * @param fileName Keytab file name in the keytab directory
     * @param principals Principals including the realm
     */
    public synchronized void createKeytab(final String fileName, final String... principals) {
        if (principals.length == 0) {
            throw new IllegalArgumentException("Principals not specified for keytab [%s]".formatted(fileName));
        }
        final String containerKeytab = CONTAINER_KEYTAB_FORMAT.formatted(fileName);
        final List<String> command = new ArrayList<>(List.of(SHELL, CREATE_KEYTAB_COMMAND, containerKeytab));
        command.addAll(Arrays.asList(principals));
        try {
            final ExecResult result = container.execInContainer(command.toArray(String[]::new));
            if (result.getExitCode() != 0) {
                throw new IllegalStateException("Keytab [%s] creation failed with exit code [%d]: %s %s".formatted(
                        fileName, result.getExitCode(), result.getStdout(), result.getStderr()));
            }
            final Path keytab = context.keytab(fileName);
            container.copyFileFromContainer(containerKeytab, keytab.toString());
            if (keytab.getFileSystem().supportedFileAttributeViews().contains(POSIX_VIEW)) {
                Files.setPosixFilePermissions(keytab, PosixFilePermissions.fromString(KEYTAB_PERMISSIONS));
            }
        } catch (final IOException e) {
            throw new UncheckedIOException("Keytab [%s] creation failed".formatted(fileName), e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Keytab [%s] creation interrupted".formatted(fileName), e);
        }
    }
}
