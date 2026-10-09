package com.frauddetection.rules;

import com.frauddetection.features.FeatureVector;

/**
 * Detects card testing patterns: multiple small probing transactions.
 */
public class CardTestingRule implements FraudRule {

    static final String NAME = "card_testing";

    @Override
    public RuleResult evaluate(FeatureVector fv) {
        double amount = fv.transaction.getAmount();
        long countInWindow = fv.txCountInWindow;

        if (amount < 1.0 && countInWindow > 3) {
            return new RuleResult(NAME, 0.8,
                    String.format("micro-transaction $%.2f with %d txns in window",
                            amount, countInWindow));
        }
        if (amount < 5.0 && countInWindow > 5) {
            return new RuleResult(NAME, 0.6,
                    String.format("small transaction $%.2f with %d txns in window",
                            amount, countInWindow));
        }
        return RuleResult.clear(NAME);
    }
}