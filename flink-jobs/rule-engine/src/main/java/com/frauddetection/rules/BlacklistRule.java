package com.frauddetection.rules;

import com.frauddetection.AccountStatus;
import com.frauddetection.EnrichedTransaction;
import com.frauddetection.MerchantRiskLevel;
import com.frauddetection.features.FeatureVector;

/**
 * Checks blacklist flags and high-risk indicators from enrichment data.
 */
public class BlacklistRule implements FraudRule {

    static final String NAME = "blacklist";

    @Override
    public RuleResult evaluate(FeatureVector fv) {
        EnrichedTransaction tx = fv.transaction;

        Boolean blacklisted = tx.getIsBlacklisted();
        if (blacklisted != null && blacklisted) {
            return new RuleResult(NAME, 1.0, "merchant is blacklisted");
        }

        AccountStatus accountStatus = tx.getAccountStatus();
        if (accountStatus == AccountStatus.SUSPENDED) {
            return new RuleResult(NAME, 0.7, "account is suspended");
        }

        MerchantRiskLevel riskLevel = tx.getMerchantRiskLevel();
        if (riskLevel == MerchantRiskLevel.HIGH) {
            return new RuleResult(NAME, 0.4, "merchant risk level is HIGH");
        }

        return RuleResult.clear(NAME);
    }
}