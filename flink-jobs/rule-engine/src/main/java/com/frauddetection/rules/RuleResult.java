package com.frauddetection.rules;

import java.io.Serializable;

/**
 * Result from a single fraud detection rule.
 * Score is 0.0 (no risk) to 1.0 (maximum risk).
 */
public class RuleResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String ruleName;
    private final double score;
    private final String explanation;

    public RuleResult(String ruleName, double score, String explanation) {
        this.ruleName = ruleName;
        this.score = Math.max(0.0, Math.min(1.0, score));
        this.explanation = explanation;
    }

    public String getRuleName() { return ruleName; }
    public double getScore() { return score; }
    public String getExplanation() { return explanation; }

    /** Convenience for rules that don't trigger. */
    public static RuleResult clear(String ruleName) {
        return new RuleResult(ruleName, 0.0, "no risk detected");
    }
}