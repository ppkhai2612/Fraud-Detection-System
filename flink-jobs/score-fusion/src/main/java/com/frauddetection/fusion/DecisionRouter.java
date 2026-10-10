package com.frauddetection.fusion;

import com.frauddetection.Decision;

import java.io.Serializable;

/**
 * Routes transactions to approve/review/decline based on the fused risk score.
 */
public class DecisionRouter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final double reviewThreshold;
    private final double declineThreshold;

    public DecisionRouter(double reviewThreshold, double declineThreshold) {
        this.reviewThreshold = reviewThreshold;
        this.declineThreshold = declineThreshold;
    }

    public Decision route(double fusedScore) {
        if (fusedScore >= declineThreshold) {
            return Decision.DECLINE;
        } else if (fusedScore >= reviewThreshold) {
            return Decision.REVIEW;
        } else {
            return Decision.APPROVE;
        }
    }
}