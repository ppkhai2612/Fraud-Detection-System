package com.frauddetection.rules;

import com.frauddetection.features.FeatureVector;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GeoAnomalyRuleTest {

    private final GeoAnomalyRule rule = new GeoAnomalyRule();

    private FeatureVector fvWithGeo(double distKm, long msLast, boolean impossible) {
        FeatureVector fv = new FeatureVector();
        fv.distanceFromLastKm = distKm;
        fv.msSinceLastTx = msLast;
        fv.impossibleTravel = impossible;
        fv.transaction = TestData.enrichedTransaction();
        return fv;
    }

    @Test
    void noTravelScoresZero() {
        RuleResult result = rule.evaluate(fvWithGeo(0, 60000, false));
        assertEquals(0.0, result.getScore());
    }

    @Test
    void impossibleTravelScores09() {
        RuleResult result = rule.evaluate(fvWithGeo(5000, 60000, true));
        assertEquals(0.9, result.getScore(), 0.01);
    }

    @Test
    void suspiciousTravelScores05() {
        // 600km in 1 hour - not impossible but suspicious
        RuleResult result = rule.evaluate(fvWithGeo(600, 60 * 60 * 1000L, false));
        assertEquals(0.5, result.getScore(), 0.01);
    }

    @Test
    void longDistanceOverLongTimeScoresZero() {
        // 600km in 3 hours - fine
        RuleResult result = rule.evaluate(fvWithGeo(600, 3 * 60 * 60 * 1000L, false));
        assertEquals(0.0, result.getScore());
    }
}