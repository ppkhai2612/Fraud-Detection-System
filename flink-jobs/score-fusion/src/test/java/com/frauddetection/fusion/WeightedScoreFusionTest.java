package com.frauddetection.fusion;

import com.frauddetection.Channel;
import com.frauddetection.EnrichedTransaction;
import com.frauddetection.TransactionType;
import com.frauddetection.rules.ScoredTransaction;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WeightedScoreFusionTest {

    private final WeightedScoreFusion fusion = new WeightedScoreFusion();

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

    private Map<String, Double> allZeroRules() {
        Map<String, Double> m = new HashMap<>();
        m.put("velocity", 0.0);
        m.put("geo_anomaly", 0.0);
        m.put("amount_threshold", 0.0);
        m.put("card_testing", 0.0);
        m.put("blacklist", 0.0);
        return m;
    }

    private Map<String, Double> allMaxRules() {
        Map<String, Double> m = new HashMap<>();
        m.put("velocity", 1.0);
        m.put("geo_anomaly", 1.0);
        m.put("amount_threshold", 1.0);
        m.put("card_testing", 1.0);
        m.put("blacklist", 1.0);
        return m;
    }

    @Test
    void allZerosWithModelZeroProducesZero() {
        assertEquals(0.0, fusion.fuse(scored(allZeroRules(), 0.0)), 0.001);
    }

    @Test
    void allMaxWithModelMaxProducesOne() {
        assertEquals(1.0, fusion.fuse(scored(allMaxRules(), 1.0)), 0.001);
    }

    @Test
    void singleRuleHighWithModelZero() {
        Map<String, Double> rules = allZeroRules();
        rules.put("blacklist", 1.0);
        // blacklist weight=0.20, model weight=0.20 with score 0.0
        // fused = (1.0*0.20 + 0.0*0.20) / (0.80 + 0.20) = 0.20
        double fused = fusion.fuse(scored(rules, 0.0));
        assertEquals(0.20, fused, 0.001);
    }

    @Test
    void modelUnavailableRenormalizesRuleWeights() {
        Map<String, Double> rules = allZeroRules();
        rules.put("blacklist", 1.0);
        // model=null, ruleWeightSum=0.80, ruleSum=1.0*0.20=0.20
        // fused = 0.20 / 0.80 = 0.25
        double fused = fusion.fuse(scored(rules, null));
        assertEquals(0.25, fused, 0.001);
    }

    @Test
    void emptyRulesNoModelReturnsZero() {
        assertEquals(0.0, fusion.fuse(scored(new HashMap<>(), null)), 0.001);
    }

    @Test
    void modelOnlyNoRules() {
        // No known rules, model score 0.8
        double fused = fusion.fuse(scored(new HashMap<>(), 0.8));
        // totalWeight = 0 + 0.20 = 0.20, ruleSum=0, fused = (0 + 0.8*0.20)/0.20 = 0.8
        assertEquals(0.8, fused, 0.001);
    }
}