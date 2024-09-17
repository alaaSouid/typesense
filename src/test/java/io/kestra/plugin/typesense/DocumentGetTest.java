package io.kestra.plugin.typesense;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This integration test retrieves a document from the Typesense Docker server.
 */
@KestraTest
class DocumentGetTest extends BaseTypeSenseTest {
    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setupCollectionAndDocument() throws Exception {
        createCompaniesCollection();

        // Prepare a document to be inserted
        Map<String, Object> document = new HashMap<>();
        document.put("id", "111");
        document.put("company_name", "Stark Industries");
        document.put("num_employees", 5215);
        document.put("country", "USA");

        // Upsert the document into the collection
        client.collections("companies").documents().upsert(document);
    }

    @Test
    void testRetrieveDocument() throws Exception {
        // Create a RunContext for the test
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        // PREPARE
        DocumentGet task = DocumentGet.builder()
            .collectionName("companies")
            .documentId("111")
            .typesenseHost(typesenseHost)
            .typesensePort(typesensePort)
            .build();

        // EXECUTE
        DocumentGet.Output runOutput = task.run(runContext);

        // VALIDATE
        assertThat(runOutput.getRetrievedDocument(), notNullValue());
        assertThat(runOutput.getRetrievedDocument().get("company_name"), is("Stark Industries"));
        assertThat(runOutput.getRetrievedDocument().get("num_employees"), is(5215));
        assertThat(runOutput.getRetrievedDocument().get("country"), is("USA"));
    }
}