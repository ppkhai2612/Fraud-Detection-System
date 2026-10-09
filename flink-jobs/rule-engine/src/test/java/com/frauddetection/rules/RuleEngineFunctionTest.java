package com.frauddetection.rules;

import com.frauddetection.features.FeatureVector;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RuleEngineFunctionTest {

    private final RuleEngineFunction function = new RuleEngineFunction();

    private FeatureVector normalFeatureVector() {
        FeatureVector fv = new FeatureVector();
        fv.transaction = TestData.enrichedTransaction();
        fv.accountId = "acct-123";
        fv.txCountInWindow = 1;
        fv.txSumTotal = 100.0;
        fv.avgAmount = 100.0;
        fv.stdDevAmount = 20.0;
        fv.msSinceLastTx = 60000;
        fv.distanceFromLastKm = 0;
        fv.impossibleTravel = false;
        return fv;
    }

    @Test
    void allFiveRulesPresent() throws Exception {
        ScoredTransaction scored = function.map(normalFeatureVector());
        assertEquals(5, scored.ruleScores.size());
        assertTrue(scored.ruleScores.containsKey("velocity"));
        assertTrue(scored.ruleScores.containsKey("geo_anomaly"));
        assertTrue(scored.ruleScores.containsKey("amount_threshold"));
        assertTrue(scored.ruleScores.containsKey("card_testing"));
        assertTrue(scored.ruleScores.containsKey("blacklist"));
    }

    @Test
    void normalTransactionScoresLow() throws Exception {
        ScoredTransaction scored = function.map(normalFeatureVector());
        assertEquals(0.0, scored.maxRuleScore());
    }

    @Test
    void scoredTransactionCarriesTransaction() throws Exception {
        ScoredTransaction scored = function.map(normalFeatureVector());
        assertNotNull(scored.transaction);
        assertEquals("acct-123", scored.accountId);
    }
}