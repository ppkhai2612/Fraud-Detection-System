package com.frauddetection.fusion;

import com.frauddetection.AlertType;
import com.frauddetection.Channel;
import com.frauddetection.Decision;
import com.frauddetection.EnrichedTransaction;
import com.frauddetection.FraudAlert;
import com.frauddetection.TransactionType;
import com.frauddetection.rules.ScoredTransaction;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AlertBuilderTest {

    private ScoredTransaction scored(Map<String, Double> ruleScores, Double modelScore) {
        EnrichedTransaction tx = EnrichedTransaction.newBuilder()
                .setTransactionId("tx-001")
                .setAccountId("acct-123")
                .setMerchantId("merch-99")
                .setAmount(100.0)
                .setCurrency("USD")
                .setTransactionType(TransactionType.PURCHASE)
                .setChannel(Channel.ONLINE)
                .setTimestamp(System.currentTimeMillis())
                .build();
        ScoredTransaction st = new ScoredTransaction(tx, ruleScores, new HashMap<>());
        st.modelScore = modelScore;
        return st;
    }

    @Test
    void velocityIsHighestReturnsVelocityType() {
        Map<String, Double> rules = new HashMap<>();
        rules.put("velocity", 0.8);
        rules.put("blacklist", 0.2);
        assertEquals(AlertType.VELOCITY, AlertBuilder.determineAlertType(scored(rules, null)));
    }

    @Test
    void blacklistIsHighestReturnsBlacklistType() {
        Map<String, Double> rules = new HashMap<>();
        rules.put("velocity", 0.1);
        rules.put("blacklist", 1.0);
        assertEquals(AlertType.BLACKLIST, AlertBuilder.determineAlertType(scored(rules, null)));
    }

    @Test
    void geoAnomalyIsHighestReturnsGeoType() {
        Map<String, Double> rules = new HashMap<>();
        rules.put("geo_anomaly", 0.9);
        rules.put("blacklist", 0.4);
        assertEquals(AlertType.GEO_ANOMALY, AlertBuilder.determineAlertType(scored(rules, null)));
    }

    @Test
    void modelDominatesReturnsModelHighRisk() {
        Map<String, Double> rules = new HashMap<>();
        rules.put("velocity", 0.3);
        assertEquals(AlertType.MODEL_HIGH_RISK, AlertBuilder.determineAlertType(scored(rules, 0.9)));
    }

    @Test
    void cardTestingReturnsCorrectType() {
        Map<String, Double> rules = new HashMap<>();
        rules.put("card_testing", 0.8);
        assertEquals(AlertType.CARD_TESTING, AlertBuilder.determineAlertType(scored(rules, null)));
    }

    @Test
    void amountThresholdReturnsCorrectType() {
        Map<String, Double> rules = new HashMap<>();
        rules.put("amount_threshold", 0.9);
        assertEquals(AlertType.AMOUNT_ANOMALY, AlertBuilder.determineAlertType(scored(rules, null)));
    }

    @Test
    void allZeroRulesNoModelDefaultsToVelocity() {
        Map<String, Double> rules = new HashMap<>();
        rules.put("velocity", 0.0);
        rules.put("blacklist", 0.0);
        assertEquals(AlertType.VELOCITY, AlertBuilder.determineAlertType(scored(rules, null)));
    }

    @Test
    void mapProducesValidFraudAlert() throws Exception {
        AlertBuilder builder = new AlertBuilder();
        Map<String, Double> rules = new HashMap<>();
        rules.put("velocity", 0.0);
        rules.put("geo_anomaly", 0.0);
        rules.put("amount_threshold", 0.0);
        rules.put("card_testing", 0.0);
        rules.put("blacklist", 1.0);

        ScoredTransaction st = scored(rules, 0.6);
        FraudAlert alert = builder.map(st);

        assertNotNull(alert.getAlertId());
        assertEquals("tx-001", alert.getTransactionId());
        assertEquals("acct-123", alert.getAccountId());
        assertEquals(AlertType.BLACKLIST, alert.getAlertType());
        assertTrue(alert.getRiskScore() > 0.0);
        assertTrue(alert.getRiskScore() <= 1.0);
        assertEquals(0.6, alert.getModelScore(), 0.001);
        assertNotNull(alert.getDecision());
        assertNotNull(alert.getExplanation());
        assertTrue(alert.getCreatedAt() > 0);
        assertEquals(5, alert.getRuleScores().size());
    }

    @Test
    void mapHighScoreProducesDecline() throws Exception {
        AlertBuilder builder = new AlertBuilder();
        Map<String, Double> rules = new HashMap<>();
        rules.put("velocity", 1.0);
        rules.put("geo_anomaly", 1.0);
        rules.put("amount_threshold", 1.0);
        rules.put("card_testing", 1.0);
        rules.put("blacklist", 1.0);

        FraudAlert alert = builder.map(scored(rules, 1.0));
        assertEquals(Decision.DECLINE, alert.getDecision());
        assertEquals(1.0, alert.getRiskScore(), 0.001);
    }

    @Test
    void mapLowScoreProducesApprove() throws Exception {
        AlertBuilder builder = new AlertBuilder();
        Map<String, Double> rules = new HashMap<>();
        rules.put("velocity", 0.0);
        rules.put("blacklist", 0.0);

        FraudAlert alert = builder.map(scored(rules, 0.0));
        assertEquals(Decision.APPROVE, alert.getDecision());
    }
}