package io.kestra.plugin.typesense;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import org.typesense.api.Client;
import io.kestra.core.models.tasks.Task;

import io.kestra.plugin.typesense.utils.UtilityMethods;
import org.typesense.model.SearchParameters;
import org.typesense.model.SearchResult;

import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Search documents in a Typesense collection and save the result as an ION file.",
    description = "This task searches for documents in a Typesense collection and saves the result in Kestra storage as an ION file."
)
@Plugin(
    examples = {
        @Example(
            title = "Search for documents",
            code = {
                "collectionName: \"companies\"",
                "searchQuery: \"Stark\"",
                "outputFileName: \"search_results.ion\""
            }
        )
    }
)
public class Search extends Task implements RunnableTask<Search.Output> {
    @Schema(
        title = "Collection name",
        description = "The name of the collection to search."
    )
    @PluginProperty(dynamic = true)
    private String collectionName;

    @Schema(
        title = "Search query",
        description = "The search query used to find documents."
    )
    @PluginProperty(dynamic = true)
    private String searchQuery;

    @Schema(
        title = "Output file name",
        description = "The name of the ION file where search results will be saved."
    )
    @PluginProperty(dynamic = true)
    private String outputFileName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // Render dynamic variables
        String renderedCollectionName = runContext.render(this.collectionName);
        String renderedSearchQuery = runContext.render(this.searchQuery);
        String renderedOutputFileName = runContext.render(this.outputFileName);

        logger.info("Searching in collection: {} with query: {}", renderedCollectionName, renderedSearchQuery);

        // Initialize Typesense client
        Client client = UtilityMethods.getTypeSenseClient();

        // Define search parameters
        SearchParameters searchParameters = new SearchParameters()
            .q(renderedSearchQuery)              // Query string
            .queryBy("company_name");    // Specify the fields to search by

        // Perform the search
        SearchResult searchResult = client.collections(renderedCollectionName).documents().search(searchParameters);

        logger.info("Search completed with [{}]", searchResult.getFound());

        // Create a temporary file for saving search results in ION format
        File tempFile = File.createTempFile(renderedOutputFileName, ".ion");

        // Write search results to the file (for simplicity, using JSON for the example)
        try (FileWriter fileWriter = new FileWriter(tempFile)) {
            fileWriter.write(searchResult.toString()); // Converting result to a string (or handle as needed)
        }

        // Store the file in Kestra's internal storage
        // TODO get the output file uri
         // URI fileUri = runContext.putTempFile(tempFile);


        // Return the output with the file URI
        // TODO return Output.builder()
        //            .fileUri(fileUri.toString())
        //            .build()
        return null;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "File URI",
            description = "The URI of the file where search results are saved."
        )
        private final String fileUri;
    }
}
