package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SaslConsumer {

    private static Logger log = LoggerFactory.getLogger(SaslConsumer.class.getSimpleName());

    private static final String VALID_KAFKA_USER = "alice";

    private static final String VALID_KAFKA_PASSWORD = "alice";

    private static final String INVALID_KAFKA_USER = "bob";

    private static final String INVALID_KAFKA_PASSWORD = "bob";

    private static final String TOPIC = "sasl_topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty("batch.size", "400");

        //SASL
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("sasl.jaas.config", PlainLoginModule.class.getName() + " required username=\"" + VALID_KAFKA_USER + "\" password=\"" + VALID_KAFKA_PASSWORD + "\";");
        properties.setProperty("group.id", "alice-group");
        properties.setProperty("auto.offset.reset", "earliest");

        //none -> If we don't have existing consumer group, we fail and have to restart the server
        //earliest -> Read from begging from the topic (--from-beginning)
        //latest -> Read only new messages (Live)
        properties.setProperty("auto.offset.reset", "earliest");

        //Create consumer
        KafkaConsumer<String, String> consumer = consumer = new KafkaConsumer<>(properties);

        //Subscribe to a topic
        consumer.subscribe(Collections.singleton(TOPIC));

        //Poll for data
        while (true) {
            ConsumerRecords<String, String> records = null;
            try {
                consumer.poll(Duration.ofMillis(1000));
            } catch (Exception e) {
                //Here is the authorization error
                log.error("ALEKSANDAR ERROR: " + e.getMessage());
            }

            for(ConsumerRecord<String, String> record: records) {
                log.info("Key: " + record.key() + ", Value: " + record.value() + ", Partition: " + record.partition(), ", Offset: " + record.offset());
            }
        }

    }
}
