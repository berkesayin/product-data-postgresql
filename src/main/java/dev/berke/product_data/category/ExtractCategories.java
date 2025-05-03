package dev.berke.product_data.category;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.ScrollResponse;

import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import dev.berke.product_data.utils.HelperMethods;
import dev.berke.product_data.utils.Utils;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@Component
@Order(1)
public class ExtractCategories implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ExtractCategories.class);
    private static final String CATEGORY_INDEX = "category";
    private static final int BATCH_SIZE = 100;
    private static final String SCROLL_KEEP_ALIVE = "1m";

    private final Utils utils;
    private final HelperMethods helperMethods;
    private final CategoryRepository categoryRepository;

    public ExtractCategories(
            Utils utils,
            HelperMethods helperMethods,
            CategoryRepository categoryRepository
    ) {
        this.utils = utils;
        this.helperMethods = helperMethods;
        this.categoryRepository = categoryRepository;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // elasticsearch authentication
        var credentialsProvider = utils.createCredentialsProvider();

        // SSL context for certificates
        SSLContext sslContext = utils.getSSLContext();

        log.info("Starting Category extraction from Elasticsearch index '{}'...", CATEGORY_INDEX);

        String scrollId = null;
        List<Category> batch = new ArrayList<>(BATCH_SIZE);
        int totalSaved = 0;
        List<Hit<Map>> currentHits;
        String lastValidScrollId = null; // store the last valid scroll ID for clearing

        // create the client locally and manage its lifecycle
        try (RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "https"))
                .setRequestConfigCallback(configBuilder -> configBuilder.setConnectTimeout(5000)
                        .setSocketTimeout(120000))
                .setHttpClientConfigCallback(builder -> builder.setSSLContext(sslContext)
                        .setDefaultCredentialsProvider(credentialsProvider)
                        .setKeepAliveStrategy((response, context) -> 120000)
                        .setMaxConnTotal(100)
                        .setMaxConnPerRoute(100)
                        .setDefaultIOReactorConfig(
                                IOReactorConfig.custom().setSoKeepAlive(true).build()))
                .build()) {

            RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            ElasticsearchClient client = new ElasticsearchClient(transport);

            SearchRequest initialSearchRequest = SearchRequest.of(s -> s
                    .index(CATEGORY_INDEX)
                    .size(BATCH_SIZE)
                    .scroll(sc -> sc.time(SCROLL_KEEP_ALIVE))
                    .query(q -> q.matchAll(m -> m))
            );

            SearchResponse<Map> searchResponse = client.search(initialSearchRequest, Map.class);
            scrollId = searchResponse.scrollId();
            lastValidScrollId = scrollId;
            currentHits = searchResponse.hits().hits();

            log.info("Initial search fetched {} category documents. Scroll ID: {}. Total hits estimate: {}",
                    currentHits != null ? currentHits.size() : 0,
                    scrollId != null ? "obtained" : "null",
                    searchResponse.hits().total() != null ? searchResponse.hits().total().value() : "unknown");

            // loop while there are hits and a valid scrollId
            while (currentHits != null && !currentHits.isEmpty() && scrollId != null) {
                log.debug("Processing batch of {} categories...", currentHits.size());
                for (Hit<Map> hit : currentHits) {
                    Map<String, Object> source = hit.source();
                    if (source == null) {
                        log.warn("Skipping hit with null source (ES ID: {})", hit.id());
                        continue;
                    }

                    try {
                        Integer categoryId = helperMethods.parseInteger(source.get("category_id"), "category_id", hit.id());
                        String categoryName = helperMethods.parseString(source.get("category_name"), "category_name", hit.id());

                        if (categoryId == null || categoryName == null) {
                            log.error("Skipping category due to missing required fields (ES ID: {}, category_id: {}, name: {}). Source: {}",
                                    hit.id(), categoryId, categoryName, source);
                            continue;
                        }
                        if (categoryRepository.existsById(categoryId)) {
                            log.debug("Category with ID {} already exists in DB. Skipping.", categoryId);
                            continue;
                        }

                        Category category = Category.builder()
                                .categoryId(categoryId)
                                .categoryName(categoryName)
                                .build();
                        batch.add(category);

                        if (batch.size() >= BATCH_SIZE) {
                            saveBatch(batch);
                            totalSaved += batch.size();
                            batch.clear();
                        }

                    } catch (Exception e) {
                        log.error("Error processing category document with ES ID: {}. Source: {}", hit.id(), source, e);
                    }
                } // end of processing current batch

                if (!batch.isEmpty()) {
                    saveBatch(batch);
                    totalSaved += batch.size();
                    batch.clear();
                }

                // fetch the next batch using Scroll API
                final String currentScrollId = scrollId;
                ScrollRequest scrollRequest = ScrollRequest.of(sr -> sr
                        .scrollId(currentScrollId)
                        .scroll(s -> s.time(SCROLL_KEEP_ALIVE))
                );

                try {
                    ScrollResponse<Map> scrollResponse = client.scroll(scrollRequest, Map.class);
                    scrollId = scrollResponse.scrollId();
                    lastValidScrollId = scrollId;
                    currentHits = scrollResponse.hits().hits();

                    if (currentHits != null && !currentHits.isEmpty()) {
                        log.debug("Scroll request fetched {} more categories. Next Scroll ID: {}", currentHits.size(), scrollId != null ? "obtained" : "null/ended");
                    } else {
                        log.debug("Scroll request returned no more hits. Ending scroll.");
                    }
                } catch (IOException e) {
                    log.error("IOException during scroll request: {}", e.getMessage(), e);
                    scrollId = null;
                    currentHits = null;
                } catch (Exception e) {
                    log.error("Error during scroll request: {}", e.getMessage(), e);
                    scrollId = null;
                    currentHits = null;
                }

            } // end of while loop (scrolling)
            log.info("Finished category extraction. Total categories saved: {}", totalSaved);

        } catch (IOException e) {
            log.error("IOException during Elasticsearch category extraction setup or initial search: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to extract categories from Elasticsearch", e);
        } catch (Exception e) {
            log.error("An unexpected error occurred during category extraction: {}", e.getMessage(), e);
            throw e;
        } finally {
            if (lastValidScrollId != null) {
                try (RestClient clearRestClient = RestClient.builder(new HttpHost("localhost", 9200, "https"))
                        .setHttpClientConfigCallback(builder -> builder.setSSLContext(sslContext)
                                .setDefaultCredentialsProvider(credentialsProvider)).build()) {

                    RestClientTransport clearTransport = new RestClientTransport(clearRestClient, new JacksonJsonpMapper());
                    ElasticsearchClient clearClient = new ElasticsearchClient(clearTransport);

                    log.info("Attempting to clear scroll context with ID: {}", lastValidScrollId);
                    final String finalScrollId = lastValidScrollId; // Effectively final for lambda
                    ClearScrollRequest clearScrollRequest = ClearScrollRequest.of(csr -> csr.scrollId(finalScrollId));

                    clearClient.clearScroll(clearScrollRequest);
                    log.info("Elasticsearch scroll context cleared successfully.");

                } catch (Exception e) {
                    log.error("Failed to clear Elasticsearch scroll context (ID: {}): {}", lastValidScrollId, e.getMessage(), e);
                }
            } else {
                log.info("No active scroll context to clear or last scroll ID was null.");
            }
        }
    }

    private void saveBatch(List<Category> batch) {
        if (!batch.isEmpty()) {
            log.info("Saving batch of {} categories to PostgreSQL...", batch.size());
            try {
                categoryRepository.saveAll(batch);
                log.debug("Successfully saved batch of {} categories.", batch.size());
            } catch (Exception e) {
                log.error("Failed to save batch of {} categories: {}", batch.size(), e.getMessage(), e);
            }
        }
    }
}