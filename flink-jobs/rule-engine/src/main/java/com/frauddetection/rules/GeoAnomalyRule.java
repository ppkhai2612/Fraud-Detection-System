package com.frauddetection.rules;

import com.frauddetection.features.FeatureVector;

/**
 * Detects impossible travel and suspicious location changes.
 */
public class GeoAnomalyRule implements FraudRule {

    static final String NAME = "geo_anomaly";
    private static final double SUSPICIOUS_DISTANCE_KM = 500.0;
    private static final long TWO_HOURS_MS = 2 * 60 * 60 * 1000L;

    @Override
    public RuleResult evaluate(FeatureVector fv) {
        if (fv.impossibleTravel) {
            return new RuleResult(NAME, 0.9,
                    String.format("impossible travel: %.0f km in %d min",
                            fv.distanceFromLastKm, fv.msSinceLastTx / 60000));
        }
        if (fv.distanceFromLastKm > SUSPICIOUS_DISTANCE_KM
                && fv.msSinceLastTx > 0 && fv.msSinceLastTx < TWO_HOURS_MS) {
            return new RuleResult(NAME, 0.5,
                    String.format("suspicious travel: %.0f km in %d min",
                            fv.distanceFromLastKm, fv.msSinceLastTx / 60000));
        }
        return RuleResult.clear(NAME);
    }
}