package com.frauddetection.enrichment;

import com.frauddetection.Channel;
import com.frauddetection.EnrichedTransaction;
import com.frauddetection.Merchant;
import com.frauddetection.MerchantRiskLevel;
import com.frauddetection.TransactionType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MerchantBroadcastFunctionTest {

    private EnrichedTransaction baseEnriched() {
        return EnrichedTransaction.newBuilder()
                .setTransactionId("tx-001")
                .setAccountId("acct-123")
                .setMerchantId("merch-99")
                .setAmount(100.0)
                .setCurrency("USD")
                .setTransactionType(TransactionType.PURCHASE)
                .setChannel(Channel.ONLINE)
                .setTimestamp(System.currentTimeMillis())
                .build();
    }

    private Merchant testMerchant() {
        return Merchant.newBuilder()
                .setMerchantId("merch-99")
                .setMerchantName("Test Electronics")
                .setCategory("electronics")
                .setCountry("US")
                .setRiskLevel(MerchantRiskLevel.LOW)
                .setIsBlacklisted(false)
                .setUpdatedAt(System.currentTimeMillis())
                .build();
    }

    @Test
    void appliesMerchantData() {
        EnrichedTransaction enriched = baseEnriched();
        Merchant merchant = testMerchant();

        MerchantBroadcastFunction.applyMerchantData(enriched, merchant);

        assertEquals("electronics", enriched.getMerchantCategory());
        assertEquals(MerchantRiskLevel.LOW, enriched.getMerchantRiskLevel());
        assertEquals(false, enriched.getIsBlacklisted());
    }

    @Test
    void appliesBlacklistedMerchant() {
        EnrichedTransaction enriched = baseEnriched();
        Merchant blacklisted = Merchant.newBuilder()
                .setMerchantId("merch-99")
                .setMerchantName("Shady Shop")
                .setCategory("gambling")
                .setCountry("XX")
                .setRiskLevel(MerchantRiskLevel.HIGH)
                .setIsBlacklisted(true)
                .setUpdatedAt(System.currentTimeMillis())
                .build();

        MerchantBroadcastFunction.applyMerchantData(enriched, blacklisted);

        assertEquals("gambling", enriched.getMerchantCategory());
        assertEquals(MerchantRiskLevel.HIGH, enriched.getMerchantRiskLevel());
        assertEquals(true, enriched.getIsBlacklisted());
    }

    @Test
    void nullMerchantLeavesFieldsUnset() {
        EnrichedTransaction enriched = baseEnriched();

        MerchantBroadcastFunction.applyMerchantData(enriched, null);

        assertNull(enriched.getMerchantCategory());
        assertNull(enriched.getMerchantRiskLevel());
        assertNull(enriched.getIsBlacklisted());
    }
}