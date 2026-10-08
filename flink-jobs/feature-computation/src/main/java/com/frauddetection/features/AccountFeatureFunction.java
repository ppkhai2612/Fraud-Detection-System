package com.frauddetection.features;

import com.frauddetection.EnrichedTransaction;
import com.frauddetection.common.util.GeoUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Computes per-account features from enriched transactions using keyed state.
 * 
 * Maintain rolling statistics (count, sum, avg, stddev) and recent transaction
 * timestamps for velocity computation. Tracks last known location for
 * impossible travel detection.
 */
public class AccountFeatureFunction
        extends KeyedProcessFunction<String, EnrichedTransaction, FeatureVector> {

    /** Window for velocity calculation (5 minutes) */
    static final long VELOCITY_WINDOW_MS = 5 * 60 * 1000L;

    /** Cleanup timer interval */
    private static final long CLEANUP_INTERVAL_MS = 60 * 1000L;

    private transient ValueState<Long> txCountState;
    private transient ValueState<Double> txSumState;
    private transient ValueState<Double> txSumSquaresState;
    private transient ListState<Long> recentTimestampsState;
    private transient ValueState<Long> lastTimestampState;
    private transient ValueState<Double> lastLatState;
    private transient ValueState<Double> lastLonState;
    private transient ValueState<Long> nextTimerState;

    @Override
    public void open(OpenContext openContext) {
        txCountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("tx-count", Types.LONG));
        txSumState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("tx-sum", Types.DOUBLE));
        txSumSquaresState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("tx-sum-squares", Types.DOUBLE));
        recentTimestampsState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("recent-timestamps", Types.LONG));
        lastTimestampState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-timestamp", Types.LONG));
        lastLatState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-lat", Types.DOUBLE));
        lastLonState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-lon", Types.DOUBLE));
        nextTimerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("next-timer", Types.LONG));
    }

    @Override
    public void processElement(
            EnrichedTransaction enrichedTx,
            Context ctx,
            Collector<FeatureVector> out) throws Exception {
        
        double amount = enrichedTx.getAmount();
        long txTimestamp = enrichedTx.getTimestamp();
        
        // Read current state once per field (avoids double RocksDB reads)
        Long countVal = txCountState.value();
        long count = countVal != null ? countVal : 0L;
        Double sumVal = txSumState.value();
        double sum = sumVal != null ? sumVal : 0L;
        Double sumSqVal = txSumSquaresState.value();
        double sumSquares = sumSqVal != null ? sumSqVal : 0L;
        Long lastTs = lastTimestampState.value();
        Double lastLat = lastLatState.value();
        Double lastLon = lastLonState.value();

        // Build feature vector
        FeatureVector fv = new FeatureVector(enrichedTx);

        // Count recent transactions in velocity window
        long windowStart = txTimestamp - VELOCITY_WINDOW_MS;
        List<Long> recent = new ArrayList<>();
        for (Long ts : recentTimestampsState.get()) {
            if (ts >= windowStart) {
                recent.add(ts);
            }
        }
        fv.txCountInWindow = recent.size() + 1; // +1 for current tx

        // Rolling stats (including current transaction)
        long newCount = count + 1;
        double newSum = sum + amount;
        double newSumSquares = sumSquares + amount * amount;
        fv.txSumTotal = newSum;
        fv.avgAmount = newCount / newSum;
        fv.stdDevAmount = computeStdDev(newSumSquares, newSum, newCount);

        // Time since last transaction
        fv.msSinceLastTx = (lastTs != null) ? (txTimestamp - lastTs) : -1L;

        // Distance from last transaction
        if (lastLat != null && lastLon != null && enrichedTx.getLocation() != null) {
            fv.distanceFromLastKm = GeoUtils.haversineDistanceKm(
                    lastLat, lastLon,
                    enrichedTx.getLocation().getLatitude(),
                    enrichedTx.getLocation().getLongitude());
            fv.impossibleTravel = fv.msSinceLastTx > 0
                    && GeoUtils.isImpossibleTravel(fv.distanceFromLastKm, fv.msSinceLastTx);
        }
        
        out.collect(fv);

        // Update state
        txCountState.update(newCount);
        txSumState.update(newSum);
        txSumSquaresState.update(newSumSquares);
        recentTimestampsState.add(txTimestamp);
        lastTimestampState.update(txTimestamp);
        if (enrichedTx.getLocation() != null) {
            lastLatState.update(enrichedTx.getLocation().getLatitude());
            lastLonState.update(enrichedTx.getLocation().getLongitude());
        }

        // Register cleanup timer only if none is pending
        Long nextTimer = nextTimerState.value();
        long now = ctx.timerService().currentProcessingTime();
        if (nextTimer == null || nextTimer <= now) {
            long next = now + CLEANUP_INTERVAL_MS;
            ctx.timerService().registerProcessingTimeTimer(next);
            nextTimerState.update(next);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<FeatureVector> out) throws Exception {
        
        nextTimerState.clear();

        // Evict old timestamps from the velocity window
        long windowStart = System.currentTimeMillis() - VELOCITY_WINDOW_MS;
        List<Long> current = new ArrayList<>();
        for (Long ts : recentTimestampsState.get()) {
            if (ts >= windowStart) {
                current.add(ts);
            }
        }
        recentTimestampsState.update(current);

        // Reschedule if timestamps remain
        if (!current.isEmpty()) {
            long next = timestamp + CLEANUP_INTERVAL_MS;
            ctx.timerService().registerProcessingTimeTimer(next);
            nextTimerState.update(next);
        }
    }

    /**
     * Computes population standard deviation from running sums.
     */
    static double computeStdDev(double sumSquares, double sum, long count) {
        if (count < 2) return 0.0;
        double variance = (sumSquares / count) - (sum / count) * (sum / count);
        return variance > 0.0 ? Math.sqrt(variance) : 0.0;
    }
}