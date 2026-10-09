package com.frauddetection.rules;

import com.frauddetection.AccountStatus;
import com.frauddetection.Channel;
import com.frauddetection.EnrichedTransaction;
import com.frauddetection.MerchantRiskLevel;
import com.frauddetection.TransactionType;
import com.frauddetection.features.FeatureVector;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BlacklistRuleTest {

    private final BlacklistRule rule = new BlacklistRule();

    private FeatureVector fvWith(Boolean blacklisted, AccountStatus status, MerchantRiskLevel risk) {
        EnrichedTransaction tx = EnrichedTransaction.newBuilder()
                .setTransactionId("tx-001")
                .setAccountId("acct-123")
                .setMerchantId("merch-99")
                .setAmount(100.0)
                .setCurrency("USD")
                .setTransactionType(TransactionType.PURCHASE)
                .setChannel(Channel.ONLINE)
                .setTimestamp(System.currentTimeMillis())
                .setIsBlacklisted(blacklisted)
                .setAccountStatus(status)
                .setMerchantRiskLevel(risk)
                .build();
        FeatureVector fv = new FeatureVector();
        fv.transaction = tx;
        return fv;
    }

    @Test
    void cleanTransactionScoresZero() {
        RuleResult result = rule.evaluate(fvWith(false, AccountStatus.ACTIVE, MerchantRiskLevel.LOW));
        assertEquals(0.0, result.getScore());
    }

    @Test
    void blacklistedMerchantScoresOne() {
        RuleResult result = rule.evaluate(fvWith(true, AccountStatus.ACTIVE, MerchantRiskLevel.LOW));
        assertEquals(1.0, result.getScore());
    }

    @Test
    void suspendedAccountScores07() {
        RuleResult result = rule.evaluate(fvWith(false, AccountStatus.SUSPENDED, MerchantRiskLevel.LOW));
        assertEquals(0.7, result.getScore(), 0.01);
    }

    @Test
    void highRiskMerchantScores04() {
        RuleResult result = rule.evaluate(fvWith(false, AccountStatus.ACTIVE, MerchantRiskLevel.HIGH));
        assertEquals(0.4, result.getScore(), 0.01);
    }

    @Test
    void blacklistTakesPriorityOverSuspended() {
        RuleResult result = rule.evaluate(fvWith(true, AccountStatus.SUSPENDED, MerchantRiskLevel.HIGH));
        assertEquals(1.0, result.getScore());
    }

    @Test
    void nullFieldsScoreZero() {
        RuleResult result = rule.evaluate(fvWith(null, null, null));
        assertEquals(0.0, result.getScore());
    }
}