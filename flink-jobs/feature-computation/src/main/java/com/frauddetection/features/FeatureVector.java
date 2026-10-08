package com.frauddetection.features;

import com.frauddetection.EnrichedTransaction;
import java.io.Serializable;

/**
 * Carries computed per-account features alongside the enriched transaction.
 * Internal pipeline type - not serialized to Kafka.
 * 
 * Flink POJO requirements: public no-arg constructor, public fields. 
 */
public class FeatureVector implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The enriched transaction being scored */
    public EnrichedTransaction transaction;
    
    /** Number of transactions in the rolling window */
    public long txCountInWindow;

    /** Lifetime sum of transaction amounts for this account */
    public double txSumTotal;

    /** Rolling average transaction amount */
    public double avgAmount;

    /** Rolling standard deviation of transaction amounts */
    public double stdDevAmount;

    /** Milliseconds since the last transaction for this account (-1 if first) */
    public long msSinceLastTx;

    /** Distance in km from the last transaction location */
    public double distanceFromLastKm;

    /** Whether the location change is flagged as impossible travel */
    public boolean impossibleTravel;

    /** Account ID (convenience for downstream keying) */
    public String accountId;

    public FeatureVector() {}

    public FeatureVector(EnrichedTransaction transaction) {
        this.transaction = transaction;
        this.accountId = transaction.getAccountId();
    }
}