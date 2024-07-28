package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SaslProducer {

    private static final Logger log = LoggerFactory.getLogger(SaslProducer.class.getSimpleName());

    private static final String INVALID_KAFKA_USER = "alice";

    private static final String INVALID_KAFKA_PASSWORD = "alice";

    private static final String VALID_KAFKA_USER = "bob";

    private static final String VALID_KAFKA_PASSWORD = "bob";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        //SASL
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("sasl.jaas.config", PlainLoginModule.class.getName() + " required username=\"" + INVALID_KAFKA_USER + "\" password=\"" + INVALID_KAFKA_PASSWORD + "\";");

        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i = 0; i < 10; i++) {
            //Create a Producer Record (message that is sent)
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("sasl_topic", "Aleksandar" + i);

            try {
                //Send data and catch callback
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception == null) {
                            // The record was successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp()
                            );
                        }
                    }
                });
            } catch (Exception e) {
                log.info("Hey hey Alesandar: " + e.getCause().getMessage());
            }
        }

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        producer.flush();
        producer.close();
    }
}
