package com.frauddetection.rules;

import com.frauddetection.features.FeatureVector;

/**
 * Detects high transaction frequency within the velocity window.
 * Threshold: more than 5 transactions in the window triggers scoring.
 */
public class VelocityRule implements FraudRule {

    static final String NAME = "velocity";
    static final int THRESHOLD = 5;

    @Override
    public RuleResult evaluate(FeatureVector fv) {
        long count = fv.txCountInWindow;
        if (count <= THRESHOLD) {
            return RuleResult.clear(NAME);
        }
        // At threshold+1: 0.5, each additional adds 0.1, capped at 1.0
        double score = 0.5 + (count - THRESHOLD - 1) * 0.1;
        return new RuleResult(NAME, score,
                String.format("%d transactions in velocity window (threshold: %d)",
                        count, THRESHOLD));
    }
}