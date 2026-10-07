package com.frauddetection.enrichment;

import com.frauddetection.Transaction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates incoming transactions. Valid transactions are emitted to the main output;
 * invalid ones are routed to the dead-letter side output as JSON strings.
 */
public class TransactionValidator extends ProcessFunction<Transaction, Transaction> {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionValidator.class);

    public static final OutputTag<String> DEAD_LETTER_TAG =
            new OutputTag<String>("dead-letter") {};

    private static final long MAX_FUTURE_MS = 5 * 60 * 1000L; // 5 minutes
    private static final long MAX_AGE_MS = 24 * 60 * 60 * 1000L; // 24 hours

    @Override
    public void processElement(Transaction tx, Context ctx, Collector<Transaction> out) {
        String error = validate(tx);
        if (error == null) {
            out.collect(tx);
        } else {
            String safeId = tx.getTransactionId() != null
                    ? jsonEscape(tx.getTransactionId()) : "null";
            String deadLetter = String.format(
                    "{\"transaction_id\":\"%s\",\"reason\":\"%s\"}",
                    safeId, error);
            LOG.warn("Invalid transaction: {}", deadLetter);
            ctx.output(DEAD_LETTER_TAG, deadLetter);
        }
    }

    private static String jsonEscape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Validate a transaction. Return null if valid, or an error message if invalid.
     */
    static String validate(Transaction tx) {
        if (tx.getTransactionId() == null || tx.getTransactionId().isEmpty()) {
            return "missing transaction_id";
        }
        if (tx.getAccountId() == null || tx.getAccountId().isEmpty()) {
            return "missing account_id";
        }
        if (tx.getMerchantId() == null || tx.getMerchantId().isEmpty()) {
            return "missing merchant_id";
        }
        if (tx.getAmount() <= 0) {
            return "amount must be positive";
        }
        long now = System.currentTimeMillis();
        if (tx.getTimestamp() > now + MAX_FUTURE_MS) {
            return "timestamp too far in the future";
        }
        if (tx.getTimestamp() < now - MAX_AGE_MS) {
            return "timestamp too old";
        }
        return null;
    }
}