package com.frauddetection.pipeline;

import com.frauddetection.Account;
import com.frauddetection.Decision;
import com.frauddetection.EnrichedTransaction;
import com.frauddetection.FraudAlert;
import com.frauddetection.Merchant;
import com.frauddetection.Transaction;
import com.frauddetection.common.config.PipelineConfig;
import com.frauddetection.common.serde.AvroSerdeFactory;
import com.frauddetection.enrichment.AccountBroadcastFunction;
import com.frauddetection.enrichment.MerchantBroadcastFunction;
import com.frauddetection.enrichment.TransactionValidator;
import com.frauddetection.features.AccountFeatureFunction;
import com.frauddetection.features.FeatureVector;
import com.frauddetection.fusion.AlertBuilder;
import com.frauddetection.model.AsyncModelClient;
import com.frauddetection.rules.RuleEngineFunction;
import com.frauddetection.rules.ScoredTransaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Fraud detection pipeline.
 * 
 * Validates transactions, enriches with account/merchant data via broadcast state,
 * computes per-account features, applies fraud detection rules, scores via ML model,
 * fuses scores, and routes alerts to Kafka topics.
 */
public class FraudDetectionPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionPipeline.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Fraud Detection Pipeline");
        LOG.info("Kafka: {}", PipelineConfig.kafkaBootstrapServers());
        LOG.info("Schema Registry: {}", PipelineConfig.schemaRegistryUrl());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Sources
        KafkaSource<Transaction> txSource = AvroSerdeFactory.kafkaSource(
                PipelineConfig.TOPIC_TRANSACTIONS,
                PipelineConfig.GROUP_FRAUD_DETECTION,
                Transaction.class);

        KafkaSource<Account> accountSource = AvroSerdeFactory.kafkaSource(
                PipelineConfig.TOPIC_ACCOUNT_UPDATES,
                PipelineConfig.GROUP_FRAUD_DETECTION + "-accounts",
                Account.class);
        
        KafkaSource<Merchant> merchantSource = AvroSerdeFactory.kafkaSource(
                PipelineConfig.TOPIC_MERCHANT_UPDATES,
                PipelineConfig.GROUP_FRAUD_DETECTION + "-merchants",
                Merchant.class);
        
        DataStream<Transaction> transactions = env.fromSource(
                txSource, WatermarkStrategy.noWatermarks(), "Kafka Transactions Source");
        
        DataStream<Account> accounts = env.fromSource(
                accountSource, WatermarkStrategy.noWatermarks(), "Kafka Account Updates Source");

        DataStream<Merchant> merchants = env.fromSource(
                merchantSource, WatermarkStrategy.noWatermarks(), "Kafka Merchant Updates Source");
        
        // Validation
        SingleOutputStreamOperator<Transaction> validTransactions = transactions
                .process(new TransactionValidator())
                .name("Transaction Validator");

        DataStream<String> deadLetters = validTransactions
                .getSideOutput(TransactionValidator.DEAD_LETTER_TAG);

        // Account Enrichment (broadcast join)
        BroadcastStream<Account> accountBroadcast =
                accounts.broadcast(AccountBroadcastFunction.ACCOUNT_STATE);
        
        SingleOutputStreamOperator<EnrichedTransaction> accountEnriched = validTransactions
                .keyBy(Transaction::getAccountId)
                .connect(accountBroadcast)
                .process(new AccountBroadcastFunction())
                .name("Accout Enrichment");

        // Merchant Enrichment (broadcast join)
        BroadcastStream<Merchant> merchantBroadcast =
                merchants.broadcast(MerchantBroadcastFunction.MERCHANT_STATE);

        SingleOutputStreamOperator<EnrichedTransaction> enrichedTransactions = accountEnriched
                .keyBy(EnrichedTransaction::getMerchantId)
                .connect(merchantBroadcast)
                .process(new MerchantBroadcastFunction())
                .name("Merchant Enrichment");

        // Feature Computation
        DataStream<FeatureVector> features = enrichedTransactions
                .keyBy(EnrichedTransaction::getAccountId)
                .process(new AccountFeatureFunction())
                .name("Feature Computation");

        // Rule Engine
        DataStream<ScoredTransaction> scored = features
                .map(new RuleEngineFunction())
                .name("Rule Engine");

        // Async Model Scoring
        DataStream<ScoredTransaction> modelScored = AsyncDataStream.unorderedWait(
                scored,
                new AsyncModelClient(),
                PipelineConfig.modelTimeoutMs(),
                TimeUnit.MILLISECONDS,
                PipelineConfig.modelMaxConcurrent())
                .name("Async Model Scoring");

        // Score Fusion + Alert Generation
        DataStream<FraudAlert> alerts = modelScored
                .map(new AlertBuilder())
                .name("Score Fusion & Alert Builder");

        // Sinks
        KafkaSink<EnrichedTransaction> enrichedSink = AvroSerdeFactory.kafkaSink(
                PipelineConfig.TOPIC_ENRICHED_TRANSACTIONS,
                EnrichedTransaction.class);
        
        enrichedTransactions
                .sinkTo(enrichedSink)
                .name("Enriched Transaction Sink");
        
        KafkaSink<String> deadLetterSink = KafkaSink.<String>builder()
                .setBootstrapServers(PipelineConfig.kafkaBootstrapServers())
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(PipelineConfig.TOPIC_DEAD_LETTER)
                                .setValueSerializationSchema(
                                        new SimpleStringSchema())
                                .build())
                .build();

        deadLetters
                .sinkTo(deadLetterSink)
                .name("Dead Letter Sink");

        // Alert Routing
        KafkaSink<FraudAlert> highRiskSink = AvroSerdeFactory.kafkaSink(
                PipelineConfig.TOPIC_ALERTS_HIGH_RISK, FraudAlert.class);

        alerts.filter(alert -> alert.getDecision() == Decision.DECLINE)
                .sinkTo(highRiskSink)
                .name("Kafka Sink: alerts-high-risk");

        KafkaSink<FraudAlert> reviewSink = AvroSerdeFactory.kafkaSink(
                PipelineConfig.TOPIC_ALERTS_REVIEW, FraudAlert.class);

        alerts.filter(alert -> alert.getDecision() == Decision.REVIEW)
                .sinkTo(reviewSink)
                .name("Kafka Sink: alerts-review");

        // Debug Logging
        alerts.map(alert -> {
            String msg = String.format(
                    "ALERT: tx=%s decision=%s score=%.3f type=%s model=%s",
                    alert.getTransactionId(),
                    alert.getDecision(),
                    alert.getRiskScore(),
                    alert.getAlertType(),
                    alert.getModelScore() != null
                            ? String.format("%.3f", alert.getModelScore()) : "N/A");
            LOG.info(msg);
            return msg;
        }).name("Alert Logger");

        env.execute("Fraud Detection Pipeline");
    }
}