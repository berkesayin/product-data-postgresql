# Extract Product And Category Data

This is a Spring Boot application to extract product and category documents from Elasticsearch and save them into PostgreSQL.

## Overview
- Extracts documents from Elasticsearch: `category` index and `product` index.
- Maps and transforms data before persisting into PostgreSQL.
- Uses JPA for database operations and Elasticsearch Java client for extraction.

## Technologies
- Java and Spring Boot
- Maven
- Elasticsearch and Kibana
- PostgreSQL Database 

## Elasticsearch Index Mapping (Schemas) 

`GET /product/_mapping`

```json
{
  "product": {
    "mappings": {
      "dynamic": "strict",
      "properties": {
        "base_price": {
          "type": "scaled_float",
          "scaling_factor": 100
        },
        "category": {
          "properties": {
            "id": {
              "type": "keyword"
            },
            "name": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword"
                }
              }
            }
          }
        },
        "created_on": {
          "type": "date"
        },
        "manufacturer": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "min_price": {
          "type": "scaled_float",
          "scaling_factor": 100
        },
        "product_id": {
          "type": "integer"
        },
        "product_name": {
          "type": "text",
          "fields": {
            "autocomplete": {
              "type": "text",
              "analyzer": "autocomplete_analyzer",
              "search_analyzer": "standard"
            },
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "sku": {
          "type": "keyword"
        },
        "status": {
          "type": "boolean"
        }
      }
    }
  }
}
```

`GET /category/_mapping`

```json
{
  "category": {
    "mappings": {
      "properties": {
        "category_id": {
          "type": "integer"
        },
        "category_name": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        }
      }
    }
  }
}
```

With extraction from Elasticsearch, category documents saved at categories table and product documents saved at products table at PostgreSQL.