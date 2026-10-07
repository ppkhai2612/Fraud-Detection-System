package com.frauddetection.enrichment;

import com.frauddetection.Account;
import com.frauddetection.EnrichedTransaction;
import com.frauddetection.Transaction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enriches transactions with account data via broadcast state.
 * Keyed by account_id. Account updates are broadcast to all parallel instances.
 */
public class AccountBroadcastFunction 
        extends KeyedBroadcastProcessFunction<String, Transaction, Account, EnrichedTransaction> {
    
    private static final Logger LOG = LoggerFactory.getLogger(KeyedBroadcastProcessFunction.class);

    public static final MapStateDescriptor<String, Account> ACCOUNT_STATE =
            new MapStateDescriptor<>(
                    "account-state",
                    Types.STRING,
                    new AvroTypeInfo<>(Account.class));
    
    @Override
    public void processElement(Transaction tx, ReadOnlyContext ctx,
                               Collector<EnrichedTransaction> out) throws Exception {
        Account account = ctx.getBroadcastState(ACCOUNT_STATE).get(tx.getAccountId());
        
        EnrichedTransaction enriched = buildEnrichedTransaction(tx, account);
        out.collect(enriched);

        if (account == null) {
			LOG.debug("No account data for account_id={}", tx.getAccountId());
        }
    }

    @Override
    public void processBroadcastElement(Account account, Context ctx,
                                        Collector<EnrichedTransaction> out) throws Exception {
        ctx.getBroadcastState(ACCOUNT_STATE).put(account.getAccountId(), account);
		LOG.info("Updated broadcast state for account_id={}", account.getAccountId());
	}

	/**
	 * Builds an EnrichedTransaction from a Transaction and optional Account data.
	 */
	static EnrichedTransaction buildEnrichedTransaction(Transaction tx, Account account) {
		EnrichedTransaction.Builder builder = EnrichedTransaction.newBuilder()
				.setTransactionId(tx.getTransactionId())
                .setAccountId(tx.getAccountId())
                .setMerchantId(tx.getMerchantId())
                .setAmount(tx.getAmount())
                .setCurrency(tx.getCurrency())
                .setTransactionType(tx.getTransactionType())
                .setChannel(tx.getChannel())
                .setLocation(tx.getLocation())
                .setTimestamp(tx.getTimestamp());
		
		if (account != null) {
            builder.setAccountType(account.getAccountType())
                   .setAccountStatus(account.getStatus())
                   .setAccountRiskScore(account.getRiskScore());
        }

		return builder.build();
	}		
}