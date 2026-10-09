package com.frauddetection.rules;

import com.frauddetection.features.FeatureVector;

import java.io.Serializable;

/**
 * Interface for fraud detection rules.
 * Each rule evaluates a feature vector and returns a score from 0.0 to 1.0.
 */
public interface FraudRule extends Serializable {
    RuleResult evaluate(FeatureVector featureVector);
}