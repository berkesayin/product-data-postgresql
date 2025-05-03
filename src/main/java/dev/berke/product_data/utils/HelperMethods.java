package dev.berke.product_data.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.format.DateTimeParseException;

@Component
public class HelperMethods {

    public static final Logger log = LoggerFactory.getLogger(HelperMethods.class);

    public Integer parseInteger(Object value, String fieldName, String esDocId) {
        if (value == null) {
            log.debug("Field '{}' is null for ES document ID {}", fieldName, esDocId);
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                log.error("Could not parse '{}' field value '{}' as Integer for ES document ID {}",
                        fieldName, value, esDocId, e);
                return null;
            }
        } else {
            log.error("Field '{}' has unexpected type {} for ES document ID {}",
                    fieldName, value.getClass().getName(), esDocId);
            return null;
        }
    }

    public BigDecimal parseBigDecimal(Object value, String fieldName, String esDocId) {
        if (value == null) {
            log.debug("Field '{}' is null for ES document ID {}", fieldName, esDocId);
            return null;
        }
        if (value instanceof Number) {
            try {
                return BigDecimal.valueOf(((Number) value).doubleValue());
            } catch (NumberFormatException e) {
                log.error("Could not convert number '{}' field value '{}' to BigDecimal for ES document ID {}",
                        fieldName, value, esDocId, e);
                return null;
            }
        } else if (value instanceof String) {
            try {
                return new BigDecimal((String) value);
            } catch (NumberFormatException e) {
                log.error("Could not parse string '{}' field value '{}' as BigDecimal for ES document ID {}",
                        fieldName, value, esDocId, e);
                return null;
            }
        } else {
            log.error("Field '{}' has unexpected type {} for ES document ID {}",
                    fieldName, value.getClass().getName(), esDocId);
            return null;
        }
    }

    public Instant parseInstant(Object value, String fieldName, String esDocId) {
        if (value == null) {
            log.debug("Field '{}' is null for ES document ID {}", fieldName, esDocId);
            return null;
        }
        if (value instanceof String) {
            try {
                return Instant.parse((String) value);
            } catch (DateTimeParseException e) {
                log.error("Could not parse date string '{}' field value '{}' as Instant for ES document ID {}",
                        fieldName, value, esDocId, e);
                return null;
            }
        }
        else if (value instanceof Number) {
            try {
                return Instant.ofEpochMilli(((Number) value).longValue());
            } catch (Exception e) {
                log.error("Could not convert number '{}' field value '{}' to Instant for ES document ID {}",
                        fieldName, value, esDocId, e);
                return null;
            }
        } else {
            log.error("Field '{}' has unexpected type {} for ES document ID {}",
                    fieldName, value.getClass().getName(), esDocId);
            return null;
        }
    }

    public String parseString(Object value, String fieldName, String esDocId) {
        if (value == null) {
            log.debug("Field '{}' is null for ES document ID {}", fieldName, esDocId);
            return null;
        }
        if (value instanceof String) {
            return (String) value; // Return even if blank, let validation handle if needed
        } else {
            log.warn("Field '{}' is not a String (type: {}) for ES document ID {}. Using toString().",
                    fieldName, value.getClass().getName(), esDocId);
            return value.toString();
        }
    }
}
