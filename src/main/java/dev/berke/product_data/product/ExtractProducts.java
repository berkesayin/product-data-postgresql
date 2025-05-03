package dev.berke.product_data.product;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;

import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import dev.berke.product_data.category.Category;
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

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import dev.berke.product_data.category.CategoryRepository;

import javax.net.ssl.SSLContext;
import java.util.Optional;

@Component
@Order(2)
public class ExtractProducts implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ExtractProducts.class);
    private static final String PRODUCT_INDEX = "product";
    private static final int BATCH_SIZE = 100;
    private static final String SCROLL_KEEP_ALIVE = "1m";

    private final Utils utils;
    private final HelperMethods helperMethods;
    private final ProductRepository productRepository;
    private final CategoryRepository categoryRepository;

    public ExtractProducts(
            Utils utils,
            HelperMethods helperMethods,
            ProductRepository productRepository,
            CategoryRepository categoryRepository
    ) {
        this.utils = utils;
        this.helperMethods = helperMethods;
        this.productRepository = productRepository;
        this.categoryRepository = categoryRepository;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // elasticsearch authentication
        var credentialsProvider = utils.createCredentialsProvider();

        // SSL context for certificates
        SSLContext sslContext = utils.getSSLContext();

        log.info("Starting Product extraction from Elasticsearch index '{}'...", PRODUCT_INDEX);

        String scrollId = null;
        List<Product> batch = new ArrayList<>(BATCH_SIZE);
        int totalSaved = 0;
        int categoryNotFoundCount = 0;
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
                    .index(PRODUCT_INDEX)
                    .size(BATCH_SIZE)
                    .scroll(sc -> sc.time(SCROLL_KEEP_ALIVE))
                    .query(q -> q.matchAll(m -> m))
            );

            SearchResponse<Map> searchResponse = client.search(initialSearchRequest, Map.class);
            scrollId = searchResponse.scrollId();
            lastValidScrollId = scrollId; // store the first valid scroll ID
            currentHits = searchResponse.hits().hits();

            log.info("Initial search fetched {} product documents. Scroll ID: {}. Total hits estimate: {}",
                    currentHits != null ? currentHits.size() : 0,
                    scrollId != null ? "obtained" : "null",
                    searchResponse.hits().total() != null ? searchResponse.hits().total().value() : "unknown");


            // loop while there are hits and a valid scrollId
            while (currentHits != null && !currentHits.isEmpty() && scrollId != null) {
                log.debug("Processing batch of {} products...", currentHits.size());
                for (Hit<Map> hit : currentHits) {
                    Map<String, Object> source = hit.source();
                    if (source == null) {
                        log.warn("Skipping hit with null source (ES ID: {})", hit.id());
                        continue;
                    }
                    try {
                        Integer productId = helperMethods.parseInteger(source.get("product_id"), "product_id", hit.id());
                        String productName = helperMethods.parseString(source.get("product_name"), "product_name", hit.id());
                        BigDecimal basePrice = helperMethods.parseBigDecimal(source.get("base_price"), "base_price", hit.id());
                        BigDecimal minPrice = helperMethods.parseBigDecimal(source.get("min_price"), "min_price", hit.id());
                        String manufacturer = helperMethods.parseString(source.get("manufacturer"), "manufacturer", hit.id());
                        String sku = helperMethods.parseString(source.get("sku"), "sku", hit.id());
                        Instant createdOn = helperMethods.parseInstant(source.get("created_on"), "created_on", hit.id());
                        Integer status = helperMethods.parseInteger(source.get("status"), "status", hit.id());
                        Integer categoryId = helperMethods.parseInteger(source.get("category_id"), "category_id", hit.id());

                        if (productId == null || productName == null || categoryId == null) {
                            log.error("Skipping product due to missing required fields (ES ID: {}, product_id: {}, name: {}, category_id: {}). Source: {}",
                                    hit.id(), productId, productName, categoryId, source);
                            continue;
                        }
                        if (productRepository.existsById(productId)) {
                            log.debug("Product with ID {} already exists in DB. Skipping.", productId);
                            continue;
                        }

                        // find the category
                        Optional<Category> categoryOpt = categoryRepository.findById(categoryId);
                        if (categoryOpt.isEmpty()) {
                            log.warn("Category with ID {} not found in database for product with ES ID {}. Skipping product.", categoryId, hit.id());
                            categoryNotFoundCount++;
                            continue;
                        }
                        Category category = categoryOpt.get();

                        // build product entity
                        Product product = Product.builder()
                                .productId(productId)
                                .productName(productName)
                                .basePrice(basePrice)
                                .minPrice(minPrice)
                                .manufacturer(manufacturer)
                                .sku(sku)
                                .createdOn(createdOn)
                                .status(status)
                                .category(category)
                                .build();

                        batch.add(product);

                        if (batch.size() >= BATCH_SIZE) {
                            saveBatch(batch);
                            totalSaved += batch.size();
                            batch.clear();
                        }

                    } catch (Exception e) {
                        log.error("Error processing product document with ES ID: {}. Source: {}", hit.id(), source, e);
                    }
                } // end of processing current batch

                // Save any remaining items in the last part of the batch before fetching next
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
                        log.debug("Scroll request fetched {} more products. Next Scroll ID: {}", currentHits.size(), scrollId != null ? "obtained" : "null/ended");
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

            log.info("Finished product extraction. Total products saved: {}. " +
                    "Products skipped due to missing category: {}", totalSaved, categoryNotFoundCount);

        } catch (IOException e) {
            log.error("IOException during Elasticsearch product extraction setup or initial search: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to extract products from Elasticsearch", e);
        } catch (Exception e) {
            log.error("An unexpected error occurred during product extraction: {}", e.getMessage(), e);
            throw e;
        } finally {
            if (lastValidScrollId != null) {
                // create a temporary client just for clearing the scroll
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

    private void saveBatch(List<Product> batch) {
        if (!batch.isEmpty()) {
            log.info("Saving batch of {} products to PostgreSQL...", batch.size());
            try {
                productRepository.saveAll(batch);
                log.debug("Successfully saved batch of {} products.", batch.size());
            } catch (Exception e) {
                log.error("Failed to save batch of {} products: {}", batch.size(), e.getMessage(), e);
            }
        }
    }
}