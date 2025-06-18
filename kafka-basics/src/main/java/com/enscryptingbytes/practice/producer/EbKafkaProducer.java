package com.enscryptingbytes.practice.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class EbKafkaProducer {
    private static Logger logger = LoggerFactory.getLogger(EbKafkaProducer.class.getSimpleName());

    public void produce() {
        logger.info("Producer without callback function.");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");

        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("kafka_project", "Kafka Value");

        kafkaProducer.send(producerRecord);

        kafkaProducer.flush();

        kafkaProducer.close();
    }

    public void produceWithCallback() {
        logger.info("Producer with callback function.");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //only for demonstration, not recommended for production
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
//        properties.setProperty("batch.size", "400");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("kafka_project", "I'm waving for the " + j + "th time now");
                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            logger.info(String.format("Topic = %s, \tPartition = %d, \tOffset = %d, \tTimestamp = %d", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp()));
                        }
                    }
                });
            }
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        kafkaProducer.flush();
        kafkaProducer.close();
    }


    public void produceWithKey() {
        logger.info("Produce with key and value");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        String topic = "kafka_project";

        for (int i = 0; i < 30; i++) {
            String key = "key" + (i % 7);
            String value = "Hello world, waving at you for the " + i + "th time!";

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info(String.format("Topic = %s, \tPartition = %d, \tOffset = %d, \tTimestamp = %d", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp()));
                    }
                }
            });
        }

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
