package com.frauddetection.features;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AccountFeatureFunctionTest {

    @Test
    void stdDevWithSingleValueIsZero() {
        assertEquals(0.0, AccountFeatureFunction.computeStdDev(100.0, 10.0, 1));
    }

    @Test
    void stdDevWithIdenticalValuesIsZero() {
        // 3 values of 10.0: sum=30, sumSquares=300
        assertEquals(0.0, AccountFeatureFunction.computeStdDev(300.0, 30.0, 3), 0.001);
    }

    @Test
    void stdDevWithVariedValues() {
        // Values: 10, 20, 30 => mean=20, variance=((100+400+900)/3 - 400) = 66.67, stddev=8.165
        double sumSq = 100.0 + 400.0 + 900.0;
        double sum = 60.0;
        double stdDev = AccountFeatureFunction.computeStdDev(sumSq, sum, 3);
        assertEquals(8.165, stdDev, 0.01);
    }

    @Test
    void stdDevNegativeVarianceClampedToZero() {
        // Floating point edge case where variance could go slightly negative
        assertEquals(0.0, AccountFeatureFunction.computeStdDev(0.0, 1.0, 2));
    }

    @Test
    void velocityWindowConstantIsFiveMinutes() {
        assertEquals(5 * 60 * 1000L, AccountFeatureFunction.VELOCITY_WINDOW_MS);
    }
}