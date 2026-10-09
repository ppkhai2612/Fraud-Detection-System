package com.frauddetection.rules;

import com.frauddetection.EnrichedTransaction;
import com.frauddetection.features.FeatureVector;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AmountThresholdRuleTest {

    private final AmountThresholdRule rule = new AmountThresholdRule();

    private FeatureVector fvWithStats(double amount, double avg, double stdDev, double sumTotal) {
        FeatureVector fv = new FeatureVector();
        EnrichedTransaction tx = TestData.enrichedTransactionWithAmount(amount);
        fv.transaction = tx;
        fv.avgAmount = avg;
        fv.stdDevAmount = stdDev;
        fv.txSumTotal = sumTotal;
        return fv;
    }

    @Test
    void normalAmountScoresZero() {
        // amount=100, avg=100, stddev=20 => z=0
        RuleResult result = rule.evaluate(fvWithStats(100, 100, 20, 500));
        assertEquals(0.0, result.getScore());
    }

    @Test
    void zeroStdDevScoresZero() {
        RuleResult result = rule.evaluate(fvWithStats(100, 100, 0, 500));
        assertEquals(0.0, result.getScore());
    }

    @Test
    void moderateOutlierScores02() {
        // amount=140, avg=100, stddev=20 => z=2.0 (boundary, just above 1.5)
        RuleResult result = rule.evaluate(fvWithStats(135, 100, 20, 500));
        assertEquals(0.2, result.getScore(), 0.01);
    }

    @Test
    void highOutlierScores05() {
        // amount=150, avg=100, stddev=20 => z=2.5
        RuleResult result = rule.evaluate(fvWithStats(150, 100, 20, 500));
        assertEquals(0.5, result.getScore(), 0.01);
    }

    @Test
    void extremeOutlierScores09() {
        // amount=170, avg=100, stddev=20 => z=3.5
        RuleResult result = rule.evaluate(fvWithStats(170, 100, 20, 500));
        assertEquals(0.9, result.getScore(), 0.01);
    }
}