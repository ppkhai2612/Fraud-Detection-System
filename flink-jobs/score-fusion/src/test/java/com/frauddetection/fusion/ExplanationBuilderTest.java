package com.frauddetection.fusion;

import com.frauddetection.Channel;
import com.frauddetection.EnrichedTransaction;
import com.frauddetection.TransactionType;
import com.frauddetection.rules.ScoredTransaction;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ExplanationBuilderTest {

    private final ExplanationBuilder builder = new ExplanationBuilder();

    private ScoredTransaction scored(
            Map<String, Double> ruleScores,
            Map<String, String> explanations,
            Double modelScore) {
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
        ScoredTransaction st = new ScoredTransaction(tx, ruleScores, explanations);
        st.modelScore = modelScore;
        return st;
    }

    @Test
    void includesFusedScoreAndDecision() {
        String result = builder.build(
                scored(new HashMap<>(), new HashMap<>(), null), 0.45, "REVIEW");
        assertTrue(result.contains("fused_score=0.450"));
        assertTrue(result.contains("decision=REVIEW"));
    }

    @Test
    void includesRuleExplanationsWithScores() {
        Map<String, Double> rules = new HashMap<>();
        rules.put("velocity", 0.7);
        Map<String, String> explanations = new HashMap<>();
        explanations.put("velocity", "8 txns in window");

        String result = builder.build(scored(rules, explanations, null), 0.7, "DECLINE");
        assertTrue(result.contains("velocity: 8 txns in window (0.70)"));
    }

    @Test
    void includesModelScore() {
        String result = builder.build(
                scored(new HashMap<>(), new HashMap<>(), 0.85), 0.5, "REVIEW");
        assertTrue(result.contains("model_score=0.850"));
    }

    @Test
    void modelUnavailableShowsUnavailable() {
        String result = builder.build(
                scored(new HashMap<>(), new HashMap<>(), null), 0.1, "APPROVE");
        assertTrue(result.contains("model_unavailable"));
    }

    @Test
    void missingRuleScoreDefaultsToZero() {
        Map<String, Double> rules = new HashMap<>();
        Map<String, String> explanations = new HashMap<>();
        explanations.put("orphan_rule", "some explanation");

        String result = builder.build(scored(rules, explanations, null), 0.0, "APPROVE");
        assertTrue(result.contains("orphan_rule: some explanation (0.00)"));
    }
}