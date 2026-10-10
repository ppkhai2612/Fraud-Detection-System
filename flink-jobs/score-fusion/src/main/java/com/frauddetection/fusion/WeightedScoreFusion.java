package com.frauddetection.fusion;

import com.frauddetection.rules.ScoredTransaction;

import java.io.Serializable;
import java.util.Map;

/**
 * Computes a weighted fusion of rule scores and model score.
 * When the model is unavailable, rule weights are renormalized.
 */
public class WeightedScoreFusion implements Serializable {

    private static final long serialVersionUID = 1L;

    static final double W_VELOCITY = 0.15;
    static final double W_GEO_ANOMALY = 0.20;
    static final double W_AMOUNT_THRESHOLD = 0.15;
    static final double W_CARD_TESTING = 0.10;
    static final double W_BLACKLIST = 0.20;
    static final double W_MODEL = 0.20;

    private static final String[] RULE_NAMES = {
            "velocity", "geo_anomaly", "amount_threshold", "card_testing", "blacklist"
    };
    private static final double[] RULE_WEIGHTS = {
            W_VELOCITY, W_GEO_ANOMALY, W_AMOUNT_THRESHOLD, W_CARD_TESTING, W_BLACKLIST
    };

    /**
     * Computes the fused risk score from rule scores and optional model score.
     * @return fused score in [0.0, 1.0]
     */
    public double fuse(ScoredTransaction scored) {
        double ruleSum = 0.0;
        double ruleWeightSum = 0.0;

        Map<String, Double> scores = scored.ruleScores;
        for (int i = 0; i < RULE_NAMES.length; i++) {
            Double ruleScore = scores.get(RULE_NAMES[i]);
            if (ruleScore != null) {
                ruleSum += ruleScore * RULE_WEIGHTS[i];
                ruleWeightSum += RULE_WEIGHTS[i];
            }
        }

        if (scored.modelScore != null) {
            double totalWeight = ruleWeightSum + W_MODEL;
            if (totalWeight == 0.0) return 0.0;
            double fusedScore = (ruleSum + scored.modelScore * W_MODEL) / totalWeight;
            return Math.max(0.0, Math.min(1.0, fusedScore));
        } else {
            if (ruleWeightSum == 0.0) return 0.0;
            double fusedScore = ruleSum / ruleWeightSum;
            return Math.max(0.0, Math.min(1.0, fusedScore));
        }
    }
}