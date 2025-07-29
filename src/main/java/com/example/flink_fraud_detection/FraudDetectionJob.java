package com.example.flink_fraud_detection;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;

public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka source setup
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("transactions")
            .setGroupId("txn-consumer-group")
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build(); // <-- You were missing this

        // Add the Kafka source
        DataStream<String> input = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "KafkaSource"
        );

        // Convert JSON strings to Transaction objects
        DataStream<Transaction> transactions = input
            .map(json -> new ObjectMapper().readValue(json, Transaction.class))
            .returns(Transaction.class);

        // Filter suspicious transactions
        DataStream<Transaction> suspicious = transactions
            .filter(tx -> tx.amount > 1000);

        // Print suspicious transactions
        suspicious.print("Suspicious");

        // Execute Flink pipeline
        env.execute("Flink Fraud Detection");
    }
}
