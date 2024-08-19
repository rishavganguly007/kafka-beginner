package org.example;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("in main()");
        // create producer properties -- upstash properties- copy pasted
        Properties props = new Properties();
        props.put("bootstrap.servers", "https://allowed-cub-14551-eu2-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"*\" password=\"*\";");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // create a Producer Record: a record that will be sent to Kafka
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("upstah-kafka", "hello-kafka");

        // send data
        producer.send(producerRecord);

        // flush and close the producer: tells the producer to send all data and block untill done -- synchronous
        producer.flush();

        // close
        producer.close();
    }
}