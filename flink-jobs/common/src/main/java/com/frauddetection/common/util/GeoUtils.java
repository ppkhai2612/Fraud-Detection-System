package com.frauddetection.common.util;

/**
 * Geographic utility functions.
 */
public final class GeoUtils {

    private static final double EARTH_RADIUS_KM = 6371.0;

    private GeoUtils() {}

    /**
     * Calculates the great-circle distance between two points using the Haversine formula.
     *
     * @return distance in kilometers
     */
    public static double haversineDistanceKm(
            double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS_KM * c;
    }

    /**
     * Checks if travel between two points in the given time is physically impossible.
     * Uses a generous max speed of 900 km/h (commercial flight speed).
     *
     * @param distanceKm distance between points
     * @param timeMs time between transactions in milliseconds
     * @return true if the travel speed exceeds 900 km/h
     */
    public static boolean isImpossibleTravel(double distanceKm, long timeMs) {
        if (timeMs <= 0) return distanceKm > 0;
        double hours = timeMs / (1000.0 * 60 * 60);
        double speedKmh = distanceKm / hours;
        return speedKmh > 900.0;
    }
}