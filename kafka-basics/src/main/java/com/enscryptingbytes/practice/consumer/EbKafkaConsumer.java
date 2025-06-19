package com.enscryptingbytes.practice.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class EbKafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(EbKafkaConsumer.class.getSimpleName());

    public static void main(String[] args) {
        EbKafkaConsumer ebKafkaConsumer = new EbKafkaConsumer();
        ebKafkaConsumer.consumeWithGracefulShutdown();
    }

    public void consume() {
        logger.info("Consume with regular configuration.");

        String groupId = "my-java-application";
        String topic = "kafka_project";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(List.of(topic));

        while (true) {
            logger.info("Consumer is Polling!!!");
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                logger.info(String.format("topic=%s\t partition=%s\t key=%s\t value=%s%n", record.topic(), record.partition(), record.key(), record.value()));
            }
        }
    }

    public void consumeWithGracefulShutdown() {
        logger.info("Consume with graceful shutdown.");

        String groupId = "my-java-application";
        String topic = "kafka_project";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        final Thread executorThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Initiated shutdown hook.");
                kafkaConsumer.wakeup();

                try {
                    executorThread.join();
                } catch (InterruptedException e) {
                    logger.info("Polling has stopped.");
                }
            }
        });


        try {
            kafkaConsumer.subscribe(List.of(topic));

            while (true) {
                logger.info("Consumer is Polling!!!");
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info(String.format("topic=%s\t partition=%s\t key=%s\t value=%s%n", record.topic(), record.partition(), record.key(), record.value()));
                }
            }
        } catch(WakeupException e) {
            logger.info("Consumer shutdown initiated.");
        } catch (Exception e) {
            logger.error("Unexpected error occurred, ", e);
        } finally {
            kafkaConsumer.close();
            logger.info("Consumer shutdown completed.");
        }
    }
}
