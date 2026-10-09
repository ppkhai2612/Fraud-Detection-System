package com.frauddetection.rules;

import com.frauddetection.Channel;
import com.frauddetection.EnrichedTransaction;
import com.frauddetection.TransactionType;

/**
 * Shared test data builders for rule tests.
 */
final class TestData {

    private TestData() {}

    static EnrichedTransaction enrichedTransaction() {
        return enrichedTransactionWithAmount(100.0);
    }

    static EnrichedTransaction enrichedTransactionWithAmount(double amount) {
        return EnrichedTransaction.newBuilder()
                .setTransactionId("tx-001")
                .setAccountId("acct-123")
                .setMerchantId("merch-99")
                .setAmount(amount)
                .setCurrency("USD")
                .setTransactionType(TransactionType.PURCHASE)
                .setChannel(Channel.ONLINE)
                .setTimestamp(System.currentTimeMillis())
                .build();
    }
}