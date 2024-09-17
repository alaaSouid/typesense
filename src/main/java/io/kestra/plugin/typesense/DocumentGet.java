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
    title = "Retrieve a document",
    description = "This task retrieves a document by its ID from a specified Typesense collection."
)
@Plugin(
    examples = {
        @Example(
            title = "Retrieve a document",
            code = {
                "collectionName: \"companies\"",
                "documentId: \"124\""
            }
        )
    }
)
public class DocumentGet extends Task implements RunnableTask<DocumentGet.Output> {
    @Schema(
        title = "Collection name",
        description = "The name of the collection from which the document will be retrieved."
    )
    @PluginProperty(dynamic = true)
    private String collectionName;

    @Schema(
        title = "Document ID",
        description = "The ID of the document to retrieve from the specified collection."
    )
    @PluginProperty(dynamic = true)
    private String documentId;

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

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // Render dynamic variables if needed
        String renderedCollectionName = runContext.render(this.collectionName);
        String renderedDocumentId = runContext.render(this.documentId);
        String typesenseHost = runContext.render(this.typesenseHost);
        int typesensePort = Integer.parseInt(runContext.render(this.typesensePort.toString()));

        logger.info("Retrieving document with ID {} from collection {}", renderedDocumentId, renderedCollectionName);

        // Initialize the Typesense client with dynamic host and port
        Client client = UtilityMethods.getTypeSenseClient(typesenseHost, typesensePort);

        // Retrieve the document by ID from the collection
        Map<String, Object> retrievedDocument = client.collections(renderedCollectionName)
            .documents(renderedDocumentId)
            .retrieve();

        logger.info("Document retrieved successfully: {}", retrievedDocument);

        // Return the retrieved document
        return Output.builder()
            .retrievedDocument(retrievedDocument)
            .build();
    }


    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Retrieved document",
            description = "The document retrieved from the Typesense collection."
        )
        private final Map<String, Object> retrievedDocument;
    }
}
