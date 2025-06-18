package com.enscryptingbytes.practice;

import com.enscryptingbytes.practice.producer.EbKafkaProducer;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello, World!");

        EbKafkaProducer kafkaProducer = new EbKafkaProducer();

        kafkaProducer.produceWithKey();
    }
}