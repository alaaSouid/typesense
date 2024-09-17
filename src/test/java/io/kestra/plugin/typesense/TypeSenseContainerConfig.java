package io.kestra.plugin.typesense;


import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

public class TypeSenseContainerConfig {
    public static final GenericContainer<?> typesenseContainer = new GenericContainer<>("typesense/typesense:0.24.0")
        .withExposedPorts(8108)
        .withEnv("TYPESENSE_API_KEY", "k8pX5hD0793d8YQC5aD1aEPd7VleSuGP")
        .withEnv("TYPESENSE_DATA_DIR", "/data")
        .withFileSystemBind("/tmp/typesense-data", "/data")
        .waitingFor(Wait.forListeningPort())
        .withStartupTimeout(Duration.ofMinutes(2));

    public static void startContainer() {
        typesenseContainer.start();
    }

    public static String getHost() {
        return typesenseContainer.getHost();
    }

    public static Integer getPort() {
        return typesenseContainer.getMappedPort(8108);
    }
}