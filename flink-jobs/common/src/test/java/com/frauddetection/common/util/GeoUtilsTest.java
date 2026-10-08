package com.frauddetection.common.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GeoUtilsTest {
    
    @Test
    void newYorkToLondonIsAbout5500km() {
        double dist = GeoUtils.haversineDistanceKm(40.7128, -74.0060, 51.5074, -0.1278);
        assertTrue(dist > 5500 && dist < 5600, "Expected ~5570km, got " + dist);
    }

    @Test
    void samePointIsZeroDistance() {
        assertEquals(0.0, GeoUtils.haversineDistanceKm(33.5, -86.8, 33.5, -86.8), 0.001);
    }

    @Test
    void impossibleTravelDetected() {
        // 5500 km in 1 hour = 5500 km/h > 900 km/h threshold
        assertTrue(GeoUtils.isImpossibleTravel(5500, 60 * 60 * 1000L));
    }

    @Test
    void possibleTravelAllowed() {
        // 500 km in 1 hour = 500 km/h < 900 km/h threshold
        assertFalse(GeoUtils.isImpossibleTravel(500, 60 * 60 * 1000L));
    }

    @Test
    void zeroTimeWithDistanceIsImpossible() {
        assertTrue(GeoUtils.isImpossibleTravel(100, 0));
    }

    @Test
    void zeroTimeZeroDistanceIsNotImpossible() {
        assertFalse(GeoUtils.isImpossibleTravel(0, 0));
    }
}