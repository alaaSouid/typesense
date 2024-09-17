package io.kestra.plugin.typesense.utils;


import org.typesense.api.Client;
import org.typesense.api.Configuration;
import org.typesense.resources.Node;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class UtilityMethods {

    private UtilityMethods () {
        throw new IllegalArgumentException();
    }

    public static Client getTypeSenseClient() {
        List<Node> nodes = new ArrayList<>();
        nodes.add(new Node("http", "localhost", "8108"));
        Configuration configuration = new Configuration(nodes, Duration.ofSeconds(2), "k8pX5hD0793d8YQC5aD1aEPd7VleSuGP");

        // Initialize the Typesense client
        return new Client(configuration);
    }

}
