package com.frauddetection.rules;

import com.frauddetection.features.FeatureVector;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CardTestingRuleTest {

    private final CardTestingRule rule = new CardTestingRule();

    private FeatureVector fvWithAmountAndCount(double amount, long count) {
        FeatureVector fv = new FeatureVector();
        fv.transaction = TestData.enrichedTransactionWithAmount(amount);
        fv.txCountInWindow = count;
        return fv;
    }

    @Test
    void normalTransactionScoresZero() {
        RuleResult result = rule.evaluate(fvWithAmountAndCount(50.0, 2));
        assertEquals(0.0, result.getScore());
    }

    @Test
    void microTransactionHighVelocityScores08() {
        // $0.50 with 4 txns in window
        RuleResult result = rule.evaluate(fvWithAmountAndCount(0.50, 4));
        assertEquals(0.8, result.getScore(), 0.01);
    }

    @Test
    void smallTransactionVeryHighVelocityScores06() {
        // $3.00 with 6 txns in window
        RuleResult result = rule.evaluate(fvWithAmountAndCount(3.0, 6));
        assertEquals(0.6, result.getScore(), 0.01);
    }

    @Test
    void microTransactionLowVelocityScoresZero() {
        // $0.50 with only 2 txns - not enough for pattern
        RuleResult result = rule.evaluate(fvWithAmountAndCount(0.50, 2));
        assertEquals(0.0, result.getScore());
    }
}