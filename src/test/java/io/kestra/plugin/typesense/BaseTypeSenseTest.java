package io.kestra.plugin.typesense;

import io.kestra.plugin.typesense.utils.UtilityMethods;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.typesense.api.Client;
import org.typesense.model.CollectionSchema;
import org.typesense.model.Field;

import java.util.ArrayList;
import java.util.List;

@Testcontainers
public abstract class BaseTypeSenseTest {
    protected static String typesenseHost;
    protected static Integer typesensePort;
    protected Client client;

    @BeforeAll
    static void startContainer() {
        TypeSenseContainerConfig.startContainer();
        typesenseHost = TypeSenseContainerConfig.getHost();
        typesensePort = TypeSenseContainerConfig.getPort();
    }

    @BeforeEach
    void setupClient() {
        client = UtilityMethods.getTypeSenseClient(typesenseHost, typesensePort);
    }

    protected void createCompaniesCollection() throws Exception {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field().name("id").type("string"));
        fields.add(new Field().name("company_name").type("string").facet(true));
        fields.add(new Field().name("num_employees").type("int32"));
        fields.add(new Field().name("country").type("string").facet(true));
        client.collections("companies").delete();

        CollectionSchema schema = new CollectionSchema();
        schema.name("companies");
        schema.fields(fields);

        // Create the collection
        client.collections().create(schema);
    }
}
