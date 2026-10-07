package com.frauddetection.enrichment;

import com.frauddetection.Channel;
import com.frauddetection.Transaction;
import com.frauddetection.TransactionType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TransactionValidatorTest {

    private Transaction.Builder validBuilder() {
        return Transaction.newBuilder()
                .setTransactionId("tx-001")
                .setAccountId("acct-123")
                .setMerchantId("merch-99")
                .setAmount(100.0)
                .setCurrency("USD")
                .setTransactionType(TransactionType.PURCHASE)
                .setChannel(Channel.ONLINE)
                .setTimestamp(System.currentTimeMillis());
    }
    
    @Test
    void validTransactionReturnsNull() {
        assertNull(TransactionValidator.validate(validBuilder().build()));
    }

    @Test
    void missingTransactionId() {
        Transaction tx = validBuilder().setTransactionId("").build();
        assertEquals("missing transaction_id", TransactionValidator.validate(tx));
    }

    @Test
    void missingAccountId() {
        Transaction tx = validBuilder().setAccountId("").build();
        assertEquals("missing account_id", TransactionValidator.validate(tx));
    }

    @Test
    void missingMerchantId() {
        Transaction tx = validBuilder().setMerchantId("").build();
        assertEquals("missing merchant_id", TransactionValidator.validate(tx));
    }

    @Test
    void zeroAmount() {
        Transaction tx = validBuilder().setAmount(0.0).build();
        assertEquals("amount must be positive", TransactionValidator.validate(tx));
    }

    @Test
    void negativeAmount() {
        Transaction tx = validBuilder().setAmount(-50.0).build();
        assertEquals("amount must be positive", TransactionValidator.validate(tx));
    }

    @Test
    void timestampTooFarInFuture() {
        long tenMinutesFromNow = System.currentTimeMillis() + 10 * 60 * 1000L;
        Transaction tx = validBuilder().setTimestamp(tenMinutesFromNow).build();
        assertEquals("timestamp too far in the future", TransactionValidator.validate(tx));
    }

    @Test
    void timestampTooOld() {
        long twoDaysAgo = System.currentTimeMillis() - 2 * 24 * 60 * 60 * 1000L;
        Transaction tx = validBuilder().setTimestamp(twoDaysAgo).build();
        assertEquals("timestamp too old", TransactionValidator.validate(tx));
    }
}