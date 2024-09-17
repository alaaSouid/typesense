package io.kestra.plugin.typesense;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.typesense.utils.UtilityMethods;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import org.typesense.api.Client;
import io.kestra.core.models.tasks.Task;

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Index a document in Typesense",
    description = "This task uses the upsert operation to either insert or update a document in a Typesense collection."
)
@Plugin(
    examples = {
        @Example(
            title = "Index a document",
            code = {
                "collectionName: \"companies\"",
                "document: {\"id\": \"124\", \"company_name\": \"Stark Industries\", \"num_employees\": 5215, \"country\": \"USA\"}"
            }
        )
    }
)
public class DocumentIndex extends Task implements RunnableTask<DocumentIndex.Output> {
    @Schema(
        title = "Collection name",
        description = "The name of the collection where the document will be indexed or updated."
    )
    @PluginProperty(dynamic = true)
    private String collectionName;

    @Schema(
        title = "Typesense Host",
        description = "The host of the Typesense server."
    )
    @PluginProperty(dynamic = true)
    private String typesenseHost;

    @Schema(
        title = "Typesense Port",
        description = "The port of the Typesense server."
    )
    @PluginProperty(dynamic = true)
    private Integer typesensePort;

    @Schema(
        title = "Document to upsert",
        description = "The document data to be indexed or updated in the specified collection."
    )
    @PluginProperty(dynamic = true)
    private Map<String, Object> document;

    public DocumentIndex.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // Render dynamic variables if needed
        String renderedCollectionName = runContext.render(this.collectionName);
        Map<String, Object> renderedDocument = runContext.render(this.document);

        logger.info("Upserting document to collection: {}", renderedCollectionName);
        logger.debug("Document content: {}", renderedDocument);

        // Initialize the Typesense client
        Client client = UtilityMethods.getTypeSenseClient(typesenseHost, typesensePort);
        // Use upsert to either insert or update the document in the collection
        Map<String, Object> upsertedDocument = client.collections(renderedCollectionName).documents().upsert(renderedDocument);

        logger.info("Document upserted successfully with response: {}", upsertedDocument);

        // Return the output with confirmation of the indexed document
        return Output.builder()
            .upsertedDocument(upsertedDocument)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Index document",
            description = "The document that was Indexed."
        )
        private final Map<String, Object> upsertedDocument;
    }
}
