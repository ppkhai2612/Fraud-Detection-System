package com.frauddetection.enrichment;

import com.frauddetection.Account;
import com.frauddetection.AccountStatus;
import com.frauddetection.AccountType;
import com.frauddetection.Channel;
import com.frauddetection.EnrichedTransaction;
import com.frauddetection.Transaction;
import com.frauddetection.TransactionType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AccountBroadcastFunctionTest {

    private Transaction testTransaction() {
        return Transaction.newBuilder()
                .setTransactionId("tx-001")
                .setAccountId("acct-123")
                .setMerchantId("merch-99")
                .setAmount(250.0)
                .setCurrency("USD")
                .setTransactionType(TransactionType.PURCHASE)
                .setChannel(Channel.ONLINE)
                .setTimestamp(System.currentTimeMillis())
                .build();
    }

    private Account testAccount() {
        return Account.newBuilder()
                .setAccountId("acct-123")
                .setCustomerId("cust-456")
                .setAccountType(AccountType.CHECKING)
                .setStatus(AccountStatus.ACTIVE)
                .setCreatedAt(System.currentTimeMillis())
                .setRiskScore(0.3)
                .setCountry("US")
                .setUpdatedAt(System.currentTimeMillis())
                .build();
    }

    @Test
    void buildsEnrichedTransactionWithAccount() {
        Transaction tx = testTransaction();
        Account account = testAccount();

        EnrichedTransaction enriched =
                AccountBroadcastFunction.buildEnrichedTransaction(tx, account);
        
        assertEquals("tx-001", enriched.getTransactionId());
        assertEquals("acct-123", enriched.getAccountId());
        assertEquals("merch-99", enriched.getMerchantId());
        assertEquals(250.0, enriched.getAmount());
        assertEquals(TransactionType.PURCHASE, enriched.getTransactionType());
        assertEquals(AccountType.CHECKING, enriched.getAccountType());
        assertEquals(AccountStatus.ACTIVE, enriched.getAccountStatus());
        assertEquals(0.3, enriched.getAccountRiskScore());
    }

    @Test
    void buildsEnrichedTransactionWithoutAccount() {
        Transaction tx = testTransaction();

        EnrichedTransaction enriched =
                AccountBroadcastFunction.buildEnrichedTransaction(tx, null);

        assertEquals("tx-001", enriched.getTransactionId());
        assertEquals(250.0, enriched.getAmount());
        assertNull(enriched.getAccountType());
        assertNull(enriched.getAccountStatus());
        assertNull(enriched.getAccountRiskScore());
    }

    @Test
    void mapsAccountStatusCorrectly() {
        Transaction tx = testTransaction();
        Account suspended = Account.newBuilder()
                .setAccountId("acct-123")
                .setCustomerId("cust-456")
                .setAccountType(AccountType.CREDIT)
                .setStatus(AccountStatus.SUSPENDED)
                .setCreatedAt(System.currentTimeMillis())
                .setCountry("US")
                .setUpdatedAt(System.currentTimeMillis())
                .build();

        EnrichedTransaction enriched =
                AccountBroadcastFunction.buildEnrichedTransaction(tx, suspended);

        assertEquals(AccountStatus.SUSPENDED, enriched.getAccountStatus());
        assertEquals(AccountType.CREDIT, enriched.getAccountType());
        assertNull(enriched.getAccountRiskScore());
    }
}