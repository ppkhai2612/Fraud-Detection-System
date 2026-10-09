package com.frauddetection.rules;

import com.frauddetection.features.FeatureVector;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VelocityRuleTest {

    private final VelocityRule rule = new VelocityRule();

    private FeatureVector fvWithCount(long count) {
        FeatureVector fv = new FeatureVector();
        fv.txCountInWindow = count;
        fv.transaction = TestData.enrichedTransaction();
        return fv;
    }

    @Test
    void belowThresholdScoresZero() {
        RuleResult result = rule.evaluate(fvWithCount(3));
        assertEquals(0.0, result.getScore());
    }

    @Test
    void atThresholdScoresZero() {
        RuleResult result = rule.evaluate(fvWithCount(VelocityRule.THRESHOLD));
        assertEquals(0.0, result.getScore());
    }

    @Test
    void oneAboveThresholdScoresHalf() {
        RuleResult result = rule.evaluate(fvWithCount(VelocityRule.THRESHOLD + 1));
        assertEquals(0.5, result.getScore(), 0.01);
    }

    @Test
    void twoAboveThresholdScores06() {
        RuleResult result = rule.evaluate(fvWithCount(VelocityRule.THRESHOLD + 2));
        assertEquals(0.6, result.getScore(), 0.01);
    }

    @Test
    void highCountCappedAtOne() {
        RuleResult result = rule.evaluate(fvWithCount(100));
        assertEquals(1.0, result.getScore());
    }
}