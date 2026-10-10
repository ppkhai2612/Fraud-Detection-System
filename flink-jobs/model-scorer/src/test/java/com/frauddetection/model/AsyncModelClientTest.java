package com.frauddetection.model;

import com.frauddetection.AccountStatus;
import com.frauddetection.AccountType;
import com.frauddetection.Channel;
import com.frauddetection.EnrichedTransaction;
import com.frauddetection.MerchantRiskLevel;
import com.frauddetection.TransactionType;
import com.frauddetection.rules.ScoredTransaction;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AsyncModelClientTest {

    private ScoredTransaction makeScoredTx() {
        EnrichedTransaction tx = EnrichedTransaction.newBuilder()
                .setTransactionId("tx-001")
                .setAccountId("acct-123")
                .setMerchantId("merch-99")
                .setAmount(142.50)
                .setCurrency("USD")
                .setTransactionType(TransactionType.PURCHASE)
                .setChannel(Channel.ONLINE)
                .setTimestamp(System.currentTimeMillis())
                .build();

        Map<String, Double> ruleScores = new HashMap<>();
        ruleScores.put("velocity", 0.5);
        ruleScores.put("blacklist", 0.0);
        Map<String, String> explanations = new HashMap<>();
        explanations.put("velocity", "6 txns in window");

        return new ScoredTransaction(tx, ruleScores, explanations);
    }

    @Test
    void buildRequestJsonContainsRequiredFields() {
        String json = AsyncModelClient.buildRequestJson(makeScoredTx());
        assertTrue(json.contains("\"transaction_id\":\"tx-001\""));
        assertTrue(json.contains("\"account_id\":\"acct-123\""));
        assertTrue(json.contains("\"amount\":142.5"));
        assertTrue(json.contains("\"transaction_type\":\"PURCHASE\""));
        assertTrue(json.contains("\"channel\":\"ONLINE\""));
    }

    @Test
    void buildRequestJsonContainsRuleScores() {
        String json = AsyncModelClient.buildRequestJson(makeScoredTx());
        assertTrue(json.contains("\"rule_scores\":{"));
        assertTrue(json.contains("\"velocity\":0.5"));
        assertTrue(json.contains("\"blacklist\":0.0"));
    }

    @Test
    void buildRequestJsonOmitsNullOptionalFields() {
        String json = AsyncModelClient.buildRequestJson(makeScoredTx());
        assertFalse(json.contains("account_type"));
        assertFalse(json.contains("account_status"));
        assertFalse(json.contains("merchant_category"));
    }

    @Test
    void parseScoreExtractsValue() {
        String body = "{\"transaction_id\":\"tx-001\",\"fraud_score\":0.87,\"model_version\":\"1.0.0\"}";
        Double score = AsyncModelClient.parseScore(body);
        assertNotNull(score);
        assertEquals(0.87, score, 0.001);
    }

    @Test
    void parseScoreReturnsNullForMissingField() {
        String body = "{\"transaction_id\":\"tx-001\",\"latency_ms\":5.2}";
        assertNull(AsyncModelClient.parseScore(body));
    }

    @Test
    void parseScoreReturnsNullForMalformedValue() {
        String body = "{\"fraud_score\":\"not_a_number\"}";
        assertNull(AsyncModelClient.parseScore(body));
    }

    @Test
    void buildRequestJsonEscapesSpecialCharacters() {
        EnrichedTransaction tx = EnrichedTransaction.newBuilder()
                .setTransactionId("tx-\"special\"")
                .setAccountId("acct-123")
                .setMerchantId("merch-99")
                .setAmount(10.0)
                .setCurrency("USD")
                .setTransactionType(TransactionType.PURCHASE)
                .setChannel(Channel.ONLINE)
                .setTimestamp(System.currentTimeMillis())
                .build();
        ScoredTransaction scored = new ScoredTransaction(tx, new HashMap<>(), new HashMap<>());
        String json = AsyncModelClient.buildRequestJson(scored);
        assertTrue(json.contains("tx-\\\"special\\\""));
    }

    @Test
    void buildRequestJsonIncludesEnrichmentFields() {
        EnrichedTransaction tx = EnrichedTransaction.newBuilder()
                .setTransactionId("tx-002")
                .setAccountId("acct-456")
                .setMerchantId("merch-50")
                .setAmount(250.0)
                .setCurrency("USD")
                .setTransactionType(TransactionType.PURCHASE)
                .setChannel(Channel.IN_STORE)
                .setTimestamp(System.currentTimeMillis())
                .setAccountType(AccountType.SAVINGS)
                .setAccountStatus(AccountStatus.ACTIVE)
                .setAccountRiskScore(0.3)
                .setMerchantCategory("Electronics & Gadgets")
                .setMerchantRiskLevel(MerchantRiskLevel.MEDIUM)
                .setIsBlacklisted(false)
                .build();
        ScoredTransaction scored = new ScoredTransaction(tx, new HashMap<>(), new HashMap<>());
        String json = AsyncModelClient.buildRequestJson(scored);
        assertTrue(json.contains("\"account_type\":\"SAVINGS\""));
        assertTrue(json.contains("\"account_status\":\"ACTIVE\""));
        assertTrue(json.contains("\"account_risk_score\":0.3"));
        assertTrue(json.contains("\"merchant_category\":\"Electronics & Gadgets\""));
        assertTrue(json.contains("\"merchant_risk_level\":\"MEDIUM\""));
        assertTrue(json.contains("\"is_blacklisted\":false"));
    }

    @Test
    void jsonEscapeHandlesControlCharacters() {
        assertEquals("line1\\nline2", AsyncModelClient.jsonEscape("line1\nline2"));
        assertEquals("col1\\tcol2", AsyncModelClient.jsonEscape("col1\tcol2"));
        assertEquals("cr\\r", AsyncModelClient.jsonEscape("cr\r"));
        assertEquals("back\\\\slash", AsyncModelClient.jsonEscape("back\\slash"));
    }

    @Test
    void parseScoreHandlesBoundaryValues() {
        assertEquals(0.0, AsyncModelClient.parseScore("{\"fraud_score\":0.0}"), 0.001);
        assertEquals(1.0, AsyncModelClient.parseScore("{\"fraud_score\":1.0}"), 0.001);
    }
}