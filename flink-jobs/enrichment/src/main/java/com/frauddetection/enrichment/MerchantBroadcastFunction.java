package com.frauddetection.enrichment;

import com.frauddetection.EnrichedTransaction;
import com.frauddetection.Merchant;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enriches transactions with merchant data via broadcast state.
 * Keyed by merchant_id. Merchant updates are broadcast to all parallel instances.
 */
public class MerchantBroadcastFunction
        extends KeyedBroadcastProcessFunction<String, EnrichedTransaction, Merchant, EnrichedTransaction> {

    private static final Logger LOG = LoggerFactory.getLogger(MerchantBroadcastFunction.class);

    public static final MapStateDescriptor<String, Merchant> MERCHANT_STATE =
            new MapStateDescriptor<>(
                    "merchant-state",
                    Types.STRING,
                    new AvroTypeInfo<>(Merchant.class));
    
    @Override
    public void processElement(EnrichedTransaction enriched, ReadOnlyContext ctx,
                               Collector<EnrichedTransaction> out) throws Exception {
        Merchant merchant = ctx.getBroadcastState(MERCHANT_STATE).get(enriched.getMerchantId());

        applyMerchantData(enriched, merchant);
        if (merchant == null) {
            LOG.debug("No merchant data for merchant_id={}", enriched.getMerchantId());
        }

        out.collect(enriched);
    }

    @Override
    public void processBroadcastElement(Merchant merchant, Context ctx,
                                        Collector<EnrichedTransaction> out) throws Exception {
        ctx.getBroadcastState(MERCHANT_STATE).put(merchant.getMerchantId(), merchant);
        LOG.info("Updated broadcast state for merchant_id={}", merchant.getMerchantId());
    }

    /**
     * Applies merchant data to an EnrichedTransaction. No-op if merchant is null.
     */
    static void applyMerchantData(EnrichedTransaction enriched, Merchant merchant) {
        if (merchant != null) {
            enriched.setMerchantCategory(merchant.getCategory());
            enriched.setMerchantRiskLevel(merchant.getRiskLevel());
            enriched.setIsBlacklisted(merchant.getIsBlacklisted());
        }
    }
}