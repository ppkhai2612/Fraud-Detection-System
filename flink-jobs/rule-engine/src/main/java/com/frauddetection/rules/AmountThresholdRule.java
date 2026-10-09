package com.frauddetection.rules;

import com.frauddetection.features.FeatureVector;

/**
 * Detects amounts that are statistical outliers for the account.
 * Uses z-score relative to the account's rolling average and standard deviation.
 */
public class AmountThresholdRule implements FraudRule {

    static final String NAME = "amount_threshold";

    @Override
    public RuleResult evaluate(FeatureVector fv) {
        if (fv.stdDevAmount <= 0 || fv.txSumTotal <= 0) {
            return RuleResult.clear(NAME);
        }
        double amount = fv.transaction.getAmount();
        double zScore = (amount - fv.avgAmount) / fv.stdDevAmount;

        if (zScore > 3.0) {
            return new RuleResult(NAME, 0.9,
                    String.format("amount $%.2f is %.1f std devs above avg $%.2f",
                            amount, zScore, fv.avgAmount));
        }
        if (zScore > 2.0) {
            return new RuleResult(NAME, 0.5,
                    String.format("amount $%.2f is %.1f std devs above avg $%.2f",
                            amount, zScore, fv.avgAmount));
        }
        if (zScore > 1.5) {
            return new RuleResult(NAME, 0.2,
                    String.format("amount $%.2f is %.1f std devs above avg $%.2f",
                            amount, zScore, fv.avgAmount));
        }
        return RuleResult.clear(NAME);
    }
}