package com.frauddetection.common.config;

/**
 * Centralized configuration for the fraud detection pipeline.
 * All values are driven by environment variables with sensible defaults
 * for running inside Docker Compose.
 */
public final class PipelineConfig {
    
    private PipelineConfig() {}
    
    // Kafka / Schema Registry
    public static String kafkaBootstrapServers() {
        return env("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
    }

    public static String schemaRegistryUrl() {
        return env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");
    }

    // Topic names
    public static final String TOPIC_TRANSACTIONS = "transactions";
    public static final String TOPIC_ACCOUNT_UPDATES = "account-updates";
    public static final String TOPIC_MERCHANT_UPDATES = "merchant-updates";
    public static final String TOPIC_ENRICHED_TRANSACTIONS = "enriched-transactions";
    public static final String TOPIC_DEAD_LETTER = "dead-letter";
    public static final String TOPIC_ALERTS_HIGH_RISK = "alerts-high-risk";
    public static final String TOPIC_ALERTS_REVIEW = "alerts-review";

    // Model service
    public static String modelEndpoint() {
        return env("MODEL_ENDPOINT", "http://model-service:8000/predict");
    }

    public static int modelTimeoutMs() {
        return Integer.parseInt(env("MODEL_TIMEOUT_MS", "2000"));
    }

    public static int modelMaxConcurrent() {
        return Integer.parseInt(env("MODEL_MAX_CONCURRENT", "100"));
    }

    // Decision Thresholds
    public static double reviewThreshold() {
        return Double.parseDouble(env("REVIEW_THRESHOLD", "0.3"));
    }

    public static double declineThreshold() {
        return Double.parseDouble(env("DECLINE_THRESHOLD", "0.7"));
    }

    // Consumer Group IDs
    public static final String GROUP_FRAUD_DETECTION = "flink-fraud-detection";

    // Helpers
    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }
}