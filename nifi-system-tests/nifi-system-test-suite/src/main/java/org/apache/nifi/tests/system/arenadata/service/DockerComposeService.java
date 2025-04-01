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
