package com.frauddetection.rules;

import com.frauddetection.EnrichedTransaction;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * An enriched transaction with all rule scores attached.
 * Output of the rule engine, consumed by ML scoring and score fusion.
 */
public class ScoredTransaction implements Serializable {

    private static final long serialVersionUID = 1L;
    
    public EnrichedTransaction transaction;
    public Map<String, Double> ruleScores;
    public Map<String, String> ruleExplanations;
    public String accountId;
    /** Model fraud probability (null if model unavailable). */
    public Double modelScore;

    public ScoredTransaction() {}

    public ScoredTransaction(
            EnrichedTransaction transaction,
            Map<String, Double> ruleScores,
            Map<String, String> ruleExplanations) {
        this.transaction = transaction;
        this.ruleScores = ruleScores != null ? ruleScores : new HashMap<>();
        this.ruleExplanations = ruleExplanations != null ? ruleExplanations : new HashMap<>();
        this.accountId = transaction.getAccountId();
    }

    /** Max score across all rules. */
    public double maxRuleScore() {
        return ruleScores.values().stream()
                .mapToDouble(Double::doubleValue)
                .max()
                .orElse(0.0);
    }

    public Map<String, Double> getRuleScores() {
        return Collections.unmodifiableMap(ruleScores);
    }
}