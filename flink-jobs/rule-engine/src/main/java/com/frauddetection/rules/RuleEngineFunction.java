package com.frauddetection.rules;

import com.frauddetection.features.FeatureVector;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies all fraud detection rules to each feature vector.
 * Collects individual rule scores into a ScoredTransaction.
 */
public class RuleEngineFunction implements MapFunction<FeatureVector, ScoredTransaction> {

    private final List<FraudRule> rules;

    public RuleEngineFunction() {
        this.rules = Arrays.asList(
                new VelocityRule(),
                new GeoAnomalyRule(),
                new AmountThresholdRule(),
                new CardTestingRule(),
                new BlacklistRule()
        );
    }

    @Override
    public ScoredTransaction map(FeatureVector fv) {
        Map<String, Double> scores = new HashMap<>();
        Map<String, String> explanations = new HashMap<>();

        for (FraudRule rule : rules) {
            RuleResult result = rule.evaluate(fv);
            scores.put(result.getRuleName(), result.getScore());
            if (result.getScore() > 0.0) {
                explanations.put(result.getRuleName(), result.getExplanation());
            }
        }

        return new ScoredTransaction(fv.transaction, scores, explanations);
    }
}