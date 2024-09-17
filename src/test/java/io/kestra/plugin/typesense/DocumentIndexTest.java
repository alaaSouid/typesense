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
 * This test will test the DocumentIndex task, ensuring the upsert operation works as expected.
 */
@KestraTest

class DocumentIndexTest extends BaseTypeSenseTest {
    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setupCollection() throws Exception {
        createCompaniesCollection();
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
            .collectionName("companies")
            .document(document)
            .typesenseHost(typesenseHost)
            .typesensePort(typesensePort)
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
