package com.frauddetection.fusion;

import com.frauddetection.Decision;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DecisionRouterTest {

    private final DecisionRouter router = new DecisionRouter(0.3, 0.7);

    @Test
    void lowScoreIsApproved() {
        assertEquals(Decision.APPROVE, router.route(0.1));
    }

    @Test
    void belowReviewThresholdIsApproved() {
        assertEquals(Decision.APPROVE, router.route(0.29));
    }

    @Test
    void atReviewThresholdIsReview() {
        assertEquals(Decision.REVIEW, router.route(0.3));
    }

    @Test
    void betweenThresholdsIsReview() {
        assertEquals(Decision.REVIEW, router.route(0.5));
    }

    @Test
    void atDeclineThresholdIsDecline() {
        assertEquals(Decision.DECLINE, router.route(0.7));
    }

    @Test
    void highScoreIsDecline() {
        assertEquals(Decision.DECLINE, router.route(1.0));
    }
}