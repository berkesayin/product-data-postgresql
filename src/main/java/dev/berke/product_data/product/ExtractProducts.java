package dev.berke.product_data.product;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import dev.berke.product_data.category.Category;
import dev.berke.product_data.category.CategoryRepository;
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
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        var credentialsProvider = utils.createCredentialsProvider();
        SSLContext sslContext = utils.getSSLContext();

        log.info("Starting Product extraction from Elasticsearch index '{}'...", PRODUCT_INDEX);

        String scrollId = null;
        String lastValidScrollId = null;

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
            lastValidScrollId = scrollId;
            List<Hit<Map>> currentHits = searchResponse.hits().hits();
            int totalSaved = 0;
            int categoryNotFoundCount = 0;
            List<Product> batch = new ArrayList<>(BATCH_SIZE);

            while (currentHits != null && !currentHits.isEmpty() && scrollId != null) {
                for (Hit<Map> hit : currentHits) {
                    Map<String, Object> source = hit.source();
                    if (source == null) {
                        log.warn("Skipping hit with null source (ES ID: {})", hit.id());
                        continue;
                    }
                    try {
                        String productIdStr = helperMethods.parseString(source.get("product_id"), "product_id", hit.id());
                        Integer productId = helperMethods.parseInteger(productIdStr, "product_id", hit.id());

                        Integer categoryId = null;
                        Object categoryObj = source.get("category");
                        if (categoryObj instanceof Map) {
                            Map<String, Object> categoryMap = (Map<String, Object>) categoryObj;
                            categoryId = helperMethods.parseInteger(categoryMap.get("id"), "category.id", hit.id());
                        }

                        Boolean status = null;
                        Object statusObj = source.get("status");
                        if (statusObj instanceof Boolean) {
                            status = (Boolean) statusObj;
                        } else if (statusObj != null) {
                            log.warn("Status for ES ID {} was not a boolean, but '{}'. Skipping status.", hit.id(), statusObj);
                        }

                        String productName = helperMethods.parseString(source.get("product_name"), "product_name", hit.id());
                        BigDecimal basePrice = helperMethods.parseBigDecimal(source.get("base_price"), "base_price", hit.id());
                        BigDecimal minPrice = helperMethods.parseBigDecimal(source.get("min_price"), "min_price", hit.id());
                        String manufacturer = helperMethods.parseString(source.get("manufacturer"), "manufacturer", hit.id());
                        String sku = helperMethods.parseString(source.get("sku"), "sku", hit.id());
                        Instant createdOn = helperMethods.parseInstant(source.get("created_on"), "created_on", hit.id());

                        if (productId == null || productName == null || categoryId == null) {
                            log.error("Skipping product due to missing required fields (ES ID: {}, product_id: {}, name: {}, category_id: {}). Source: {}",
                                    hit.id(), productId, productName, categoryId, source);
                            continue;
                        }
                        if (productRepository.existsById(productId)) {
                            log.debug("Product with ID {} already exists in DB. Skipping.", productId);
                            continue;
                        }

                        Optional<Category> categoryOpt = categoryRepository.findById(categoryId);
                        if (categoryOpt.isEmpty()) {
                            log.warn("Category with ID {} not found in database for product with ES ID {}. Skipping product.", categoryId, hit.id());
                            categoryNotFoundCount++;
                            continue;
                        }

                        Product product = Product.builder()
                                .productId(productId)
                                .productName(productName)
                                .basePrice(basePrice)
                                .minPrice(minPrice)
                                .manufacturer(manufacturer)
                                .sku(sku)
                                .createdOn(createdOn)
                                .status(status)
                                .category(categoryOpt.get())
                                .build();

                        batch.add(product);

                    } catch (Exception e) {
                        log.error("Error processing product document with ES ID: {}. Source: {}", hit.id(), source, e);
                    }
                }

                if (!batch.isEmpty()) {
                    saveBatch(batch);
                    totalSaved += batch.size();
                    batch.clear();
                }

                // scroll to next batch
                final String currentScrollId = scrollId;
                ScrollRequest scrollRequest = ScrollRequest.of(sr -> sr.scrollId(currentScrollId).scroll(s -> s.time(SCROLL_KEEP_ALIVE)));
                ScrollResponse<Map> scrollResponse = client.scroll(scrollRequest, Map.class);
                scrollId = scrollResponse.scrollId();
                lastValidScrollId = scrollId;
                currentHits = scrollResponse.hits().hits();
            }

            log.info("Finished product extraction. Total products saved: {}. " +
                    "Products skipped due to missing category: {}", totalSaved, categoryNotFoundCount);

        } finally {
            if (lastValidScrollId != null) {
                // clear scroll context
                try (RestClient clearRestClient = RestClient.builder(new HttpHost("localhost", 9200, "https"))
                        .setHttpClientConfigCallback(builder -> builder.setSSLContext(sslContext)
                                .setDefaultCredentialsProvider(credentialsProvider)).build()) {
                    RestClientTransport clearTransport = new RestClientTransport(clearRestClient, new JacksonJsonpMapper());
                    ElasticsearchClient clearClient = new ElasticsearchClient(clearTransport);

                    final String finalScrollId = lastValidScrollId;

                    clearClient.clearScroll(csr -> csr.scrollId(finalScrollId));
                    log.info("Elasticsearch scroll context cleared successfully.");
                } catch (Exception e) {
                    log.error("Failed to clear Elasticsearch scroll context (ID: {}): {}", lastValidScrollId, e.getMessage(), e);
                }
            }
        }
    }

    private void saveBatch(List<Product> batch) {
        if (!batch.isEmpty()) {
            log.info("Saving batch of {} products to PostgreSQL...", batch.size());
            try {
                productRepository.saveAll(batch);
            } catch (Exception e) {
                log.error("Failed to save batch of {} products: {}", batch.size(), e.getMessage(), e);
            }
        }
    }
}