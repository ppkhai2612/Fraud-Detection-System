package com.frauddetection.fusion;

import com.frauddetection.AlertType;
import com.frauddetection.Decision;
import com.frauddetection.FraudAlert;
import com.frauddetection.common.config.PipelineConfig;
import com.frauddetection.rules.ScoredTransaction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Builds FraudAlert Avro records from scored transactions.
 * Applies score fusion, decision routing and explanation generation.
 * Exposes custom Flink counters for alert decisions and types.
 */
public class AlertBuilder extends RichMapFunction<ScoredTransaction, FraudAlert> {

    private static final long serialVersionUID = 1L;

    private final WeightedScoreFusion fusion;
    private final DecisionRouter router;
    private final ExplanationBuilder explanationBuilder;

    private transient Counter alertsTotal;
    private transient Counter alertsApprove;
    private transient Counter alertsReview;
    private transient Counter alertsDecline;
    private transient Counter alertsVelocity;
    private transient Counter alertsGeoAnomaly;
    private transient Counter alertsAmountAnomaly;
    private transient Counter alertsCardTesting;
    private transient Counter alertsBlacklist;
    private transient Counter alertsModelHighRisk;

    private transient Counter rulesVelocityTriggered;
    private transient Counter rulesGeoAnomalyTriggered;
    private transient Counter rulesAmountAnomalyTriggered;
    private transient Counter rulesCardTestingTriggered;
    private transient Counter rulesBlacklistTriggered;
    private transient Counter rulesModelHighRiskTriggered;

    public AlertBuilder() {
        this.fusion = new WeightedScoreFusion();
        this.router = new DecisionRouter(
                PipelineConfig.reviewThreshold(),
                PipelineConfig.declineThreshold());
        this.explanationBuilder = new ExplanationBuilder();
    }

    @Override
    public void open(OpenContext openContext) {
        alertsTotal = getRuntimeContext().getMetricGroup().counter("alerts_total");
        alertsApprove = getRuntimeContext().getMetricGroup().counter("alerts_approve");
        alertsReview = getRuntimeContext().getMetricGroup().counter("alerts_review");
        alertsDecline = getRuntimeContext().getMetricGroup().counter("alerts_decline");
        alertsVelocity = getRuntimeContext().getMetricGroup().counter("alerts_velocity");
        alertsGeoAnomaly = getRuntimeContext().getMetricGroup().counter("alerts_geo_anomaly");
        alertsAmountAnomaly = getRuntimeContext().getMetricGroup().counter("alerts_amount_anomaly");
        alertsCardTesting = getRuntimeContext().getMetricGroup().counter("alerts_card_testing");
        alertsBlacklist = getRuntimeContext().getMetricGroup().counter("alerts_blacklist");
        alertsModelHighRisk = getRuntimeContext().getMetricGroup().counter("alerts_model_high_risk");

        rulesVelocityTriggered = getRuntimeContext().getMetricGroup().counter("rules_velocity_triggered");
        rulesGeoAnomalyTriggered = getRuntimeContext().getMetricGroup().counter("rules_geo_anomaly_triggered");
        rulesAmountAnomalyTriggered = getRuntimeContext().getMetricGroup().counter("rules_amount_anomaly_triggered");
        rulesCardTestingTriggered = getRuntimeContext().getMetricGroup().counter("rules_card_testing_triggered");
        rulesBlacklistTriggered = getRuntimeContext().getMetricGroup().counter("rules_blacklist_triggered");
        rulesModelHighRiskTriggered = getRuntimeContext().getMetricGroup().counter("rules_model_high_risk_triggered");
    }

    @Override
    public FraudAlert map(ScoredTransaction scored) {
        double fusedScore = fusion.fuse(scored);
        Decision decision = router.route(fusedScore);
        AlertType alertType = determineAlertType(scored);
        String explanation = explanationBuilder.build(
            scored, fusedScore, decision.toString());

        Map<String, Double> ruleScoresCopy = new HashMap<>(scored.ruleScores);
        
        FraudAlert alert = FraudAlert.newBuilder()
                .setAlertId(UUID.randomUUID().toString())
                .setTransactionId(scored.transaction.getTransactionId())
                .setAccountId(scored.transaction.getAccountId())
                .setAlertType(alertType)
                .setRiskScore(fusedScore)
                .setRuleScores(ruleScoresCopy)
                .setModelScore(scored.modelScore)
                .setDecision(decision)
                .setExplanation(explanation)
                .setCreatedAt(System.currentTimeMillis())
                .build();
        
        incrementCounters(alert);
        return alert;
    }

    private void incrementCounters(FraudAlert alert) {
        if (alertsTotal == null) return; // not initialized (unit test context)

        alertsTotal.inc();

        switch (alert.getDecision()) {
            case APPROVE -> alertsApprove.inc();
            case REVIEW -> alertsReview.inc();
            case DECLINE -> alertsDecline.inc();
        }

        switch (alert.getAlertType()) {
            case VELOCITY -> alertsVelocity.inc();
            case GEO_ANOMALY -> alertsGeoAnomaly.inc();
            case AMOUNT_ANOMALY -> alertsAmountAnomaly.inc();
            case CARD_TESTING -> alertsCardTesting.inc();
            case BLACKLIST -> alertsBlacklist.inc();
            case MODEL_HIGH_RISK -> alertsModelHighRisk.inc();
        }

        Map<String, Double> scores = alert.getRuleScores();
        if (scores != null) {
            incrementIfPositive(scores.get("velocity"), rulesVelocityTriggered);
            incrementIfPositive(scores.get("geo_anomaly"), rulesGeoAnomalyTriggered);
            incrementIfPositive(scores.get("amount_threshold"), rulesAmountAnomalyTriggered);
            incrementIfPositive(scores.get("card_testing"), rulesCardTestingTriggered);
            incrementIfPositive(scores.get("blacklist"), rulesBlacklistTriggered);
        }
        if (alert.getModelScore() != null && alert.getModelScore() > 0.0) {
            rulesModelHighRiskTriggered.inc();
        }
    }

    private static void incrementIfPositive(Double score, Counter counter) {
        if (score != null && score > 0.0) {
            counter.inc();
        }
    }

    static AlertType determineAlertType(ScoredTransaction scored) {
        String highestRule = null;
        double highestScore = 0.0;

        for (Map.Entry<String, Double> entry : scored.ruleScores.entrySet()) {
            if (entry.getValue() > highestScore) {
                highestScore = entry.getValue();
                highestRule = entry.getKey();
            }
        }

        if (scored.modelScore != null && scored.modelScore > highestScore) {
            return AlertType.MODEL_HIGH_RISK;
        }

        if (highestRule == null) return AlertType.VELOCITY;

        return switch (highestRule) {
            case "velocity" -> AlertType.VELOCITY;
            case "geo_anomaly" -> AlertType.GEO_ANOMALY;
            case "amount_threshold" -> AlertType.AMOUNT_ANOMALY;
            case "card_testing" -> AlertType.CARD_TESTING;
            case "blacklist" -> AlertType.BLACKLIST;
            default -> AlertType.VELOCITY;
        };
    }
}