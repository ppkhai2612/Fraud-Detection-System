package com.frauddetection.model;

import com.frauddetection.EnrichedTransaction;
import com.frauddetection.common.config.PipelineConfig;
import com.frauddetection.rules.ScoredTransaction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

/**
 * Async I/O function that scores transactions via the ML model service.
 * On timeout or error, falls back to rule-only scoring (modelScore remains null).
 */
public class AsyncModelClient extends RichAsyncFunction<ScoredTransaction, ScoredTransaction> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncModelClient.class);

    private transient HttpClient httpClient;
    private final String modelEndpoint;
    private final Duration timeout;

    public AsyncModelClient() {
        this.modelEndpoint = PipelineConfig.modelEndpoint();
        this.timeout = Duration.ofMillis(PipelineConfig.modelTimeoutMs());
    }

    @Override
    public void open(OpenContext openContext) {
        httpClient = HttpClient.newBuilder()
                .connectTimeout(timeout)
                .build();
        LOG.info("AsyncModelClient initialized. Endpoint: {}, timeout: {}ms",
                modelEndpoint, timeout.toMillis());
    }

    @Override
    public void asyncInvoke(
            ScoredTransaction scored,
            ResultFuture<ScoredTransaction> resultFuture) {

        String requestBody = buildRequestJson(scored);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(modelEndpoint))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> {
                    if (response.statusCode() == 200) {
                        Double modelScore = parseScore(response.body());
                        scored.modelScore = modelScore;
                        LOG.debug("Model score for tx={}: {}",
                                scored.transaction.getTransactionId(), modelScore);
                    } else {
                        LOG.warn("Model returned status {} for tx={}",
                                response.statusCode(),
                                scored.transaction.getTransactionId());
                    }
                    resultFuture.complete(Collections.singleton(scored));
                })
                .exceptionally(throwable -> {
                    LOG.warn("Model call failed for tx={}: {}",
                            scored.transaction.getTransactionId(),
                            throwable.getMessage());
                    resultFuture.complete(Collections.singleton(scored));
                    return null;
                });
    }

    @Override
    public void close() {
        httpClient = null;
    }

    @Override
    public void timeout(
            ScoredTransaction scored,
            ResultFuture<ScoredTransaction> resultFuture) {
        LOG.warn("Model timeout for tx={}", scored.transaction.getTransactionId());
        resultFuture.complete(Collections.singleton(scored));
    }
    
    static String buildRequestJson(ScoredTransaction scored) {
        EnrichedTransaction tx = scored.transaction;
        StringBuilder sb = new StringBuilder(512);
        sb.append("{");
        appendString(sb, "transaction_id", tx.getTransactionId());
        sb.append(",");
        appendString(sb, "account_id", tx.getAccountId());
        sb.append(",\"amount\":").append(tx.getAmount());
        sb.append(",\"transaction_type\":\"").append(tx.getTransactionType()).append("\"");
        sb.append(",\"channel\":\"").append(tx.getChannel()).append("\"");

        if (tx.getAccountType() != null) {
            sb.append(",\"account_type\":\"").append(tx.getAccountType()).append("\"");
        }
        if (tx.getAccountStatus() != null) {
            sb.append(",\"account_status\":\"").append(tx.getAccountStatus()).append("\"");
        }
        if (tx.getAccountRiskScore() != null) {
            sb.append(",\"account_risk_score\":").append(tx.getAccountRiskScore());
        }
        if (tx.getMerchantCategory() != null) {
            sb.append(",\"merchant_category\":\"").append(jsonEscape(tx.getMerchantCategory())).append("\"");
        }
        if (tx.getMerchantRiskLevel() != null) {
            sb.append(",\"merchant_risk_level\":\"").append(tx.getMerchantRiskLevel()).append("\"");
        }
        if (tx.getIsBlacklisted() != null) {
            sb.append(",\"is_blacklisted\":").append(tx.getIsBlacklisted());
        }

        sb.append(",\"rule_scores\":{");
        boolean first = true;
        for (Map.Entry<String, Double> entry : scored.ruleScores.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(entry.getKey()).append("\":").append(entry.getValue());
            first = false;
        }
        sb.append("}");

        sb.append("}");
        return sb.toString();
    }

    private static void appendString(StringBuilder sb, String key, String value) {
        sb.append("\"").append(key).append("\":\"").append(jsonEscape(value)).append("\"");
    }

    static String jsonEscape(String s) {
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    static Double parseScore(String responseBody) {
        int idx = responseBody.indexOf("\"fraud_score\"");
        if (idx < 0) return null;
        int colonIdx = responseBody.indexOf(":", idx);
        if (colonIdx < 0) return null;
        int start = colonIdx + 1;
        while (start < responseBody.length() && responseBody.charAt(start) == ' ') start++;
        int end = start;
        while (end < responseBody.length()
                && (Character.isDigit(responseBody.charAt(end))
                || responseBody.charAt(end) == '.'
                || responseBody.charAt(end) == '-')) {
            end++;
        }
        try {
            return Double.parseDouble(responseBody.substring(start, end));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}