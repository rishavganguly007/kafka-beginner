package org.example;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("in main()");
        // create producer properties -- upstash properties- copy pasted
        Properties props = new Properties();
        props.put("bootstrap.servers", "allowed-cub-14551-eu2-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"*****\" password=\"****\";");
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "YOUR_CONSUMER_GROUP");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        // create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // subscribe to a topic
        consumer.subscribe(Arrays.asList("upstah-kafka"));

        // pull data from topic
        while(true) {
            log.info("polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + ", value: " + record.value());
                log.info("partition: " + record.partition() + ", offset: " + record.offset());
            }

        }
    }
}