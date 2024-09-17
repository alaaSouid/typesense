package io.kestra.plugin.typesense;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.typesense.utils.UtilityMethods;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.typesense.api.Client;
import org.typesense.model.CollectionSchema;
import org.typesense.model.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This test will test the DocumentIndex task, ensuring the upsert operation works as expected.
 */
@KestraTest
class DocumentIndexTest {
    @Inject
    private RunContextFactory runContextFactory;
    private Client client;

    @BeforeEach
    void setup() {
        // Initialize Typesense
        client = UtilityMethods.getTypeSenseClient();

        try {
            List<Field> fields = new ArrayList<>();

            // DEFINE FIELDS
            fields.add(new Field().name("id").type("string"));
            fields.add(new Field().name("company_name").type("string").facet(true));
            fields.add(new Field().name("num_employees").type("int32"));
            fields.add(new Field().name("country").type("string").facet(true));

            CollectionSchema schema = new CollectionSchema();
            schema.name("companies");
            schema.fields(fields);
            schema.defaultSortingField("num_employees");

            // PREPARE COLLECTION
            client.collections().create(schema);
        } catch (Exception e) {
            System.out.println("Collection 'companies' already exists, skipping creation.");
        }
    }
    @Test
    void testDocumentUpsert() throws Exception {
        // Create a RunContext for the test
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        // PREPARE
        Map<String, Object> document = new HashMap<>();
        document.put("id", "124");
        document.put("company_name", "Stark Industries");
        document.put("num_employees", 5215);
        document.put("country", "USA");


        DocumentIndex task = DocumentIndex.builder()
            .collectionName("companies") // Define the collection name
            .document(document) // Define the document to upsert
            .build();

        // EXECUTE
        DocumentIndex.Output runOutput = task.run(runContext);

        // VALIDATE
        assertThat(runOutput.getUpsertedDocument(), notNullValue());
        assertThat(runOutput.getUpsertedDocument().get("company_name"), is("Stark Industries"));
        assertThat(runOutput.getUpsertedDocument().get("num_employees"), is(5215));
        assertThat(runOutput.getUpsertedDocument().get("country"), is("USA"));
    }
}
