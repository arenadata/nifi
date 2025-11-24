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
package org.apache.nifi.tests.system.arenadata.service;

import lombok.Setter;
import org.apache.nifi.tests.system.arenadata.model.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.time.Duration;
import java.util.List;

@Setter
public class DockerComposeService {

    private static final Duration DEFAULT_STARTUP_TIMEOUT = Duration.ofMinutes(10);
    private static final String DEFAULT_COMPOSE_FILE_NAME = "env/arenadata/docker-compose.yaml";
    private static final Logger logger = LoggerFactory.getLogger(DockerComposeService.class);

    private final List<Component> components;
    private DockerComposeContainer<?> compose;

    static {
        System.setProperty("docker.client.strategy", "org.testcontainers.dockerclient.DockerClientProviderStrategy");
    }

    public DockerComposeService(List<Component> components) {
        this.components = components;
    }

    public void init() {
        long millisBeforeStart = System.currentTimeMillis();
        logger.info("##### STARTING TEST CONTAINERS... #####");
        compose = new DockerComposeContainer<>(new File(DEFAULT_COMPOSE_FILE_NAME));
        for (Component component : components) {
            compose.withExposedService(component.getName(), component.getPort(),
                    Wait.forHealthcheck().withStartupTimeout(DEFAULT_STARTUP_TIMEOUT));
        }
        compose.withPull(true).start();
        logger.info("##### TEST CONTAINERS HAVE STARTED IN {} sec #####", (System.currentTimeMillis() - millisBeforeStart) / 1000);
    }
}
