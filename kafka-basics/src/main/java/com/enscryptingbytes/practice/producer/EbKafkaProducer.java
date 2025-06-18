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
}
