package com.frauddetection.fusion;

import com.frauddetection.rules.ScoredTransaction;

import java.io.Serializable;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Builds human-readable explanations for fraud alerts.
 */
public class ExplanationBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    public String build(ScoredTransaction scored, double fusedScore, String decision) {
        StringJoiner sj = new StringJoiner("; ");
        sj.add(String.format("fused_score=%.3f decision=%s", fusedScore, decision));

        for (Map.Entry<String, String> entry : scored.ruleExplanations.entrySet()) {
            sj.add(String.format("%s: %s (%.2f)",
                    entry.getKey(), entry.getValue(),
                    scored.ruleScores.getOrDefault(entry.getKey(), 0.0)));
        }

        if (scored.modelScore != null) {
            sj.add(String.format("model_score=%.3f", scored.modelScore));
        } else {
            sj.add("model_unavailable");
        }

        return sj.toString();
    }
}