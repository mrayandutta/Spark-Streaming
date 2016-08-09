package com.xpandit.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;

public class KafkaProducerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerService.class);

    private static final long RETRY_INTERVAL_MS = 6000L;
    private static final int MAX_RETRIES = 10;


    private Producer<String, String> producer;
    private int controlTopicPartitions;

    private String zookeeper;
    private String brokers;
    private String topic;


    public KafkaProducerService(String zookeeper, String brokers, String topic){
        this.zookeeper = zookeeper;
        this.brokers = brokers;
        this.topic = topic;

        init();
    }


    public void init() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", this.brokers);
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //kafkaProperties.put("acks", "all");
        this.producer = new KafkaProducer<>(kafkaProperties);
        KafkaAdmin admin = new KafkaAdmin(this.zookeeper, "/");
        this.controlTopicPartitions = admin.getTopicMetadata(this.topic).partitionMetadata().size();
        admin.close();

        LOGGER.info("Kafka topic '{}' has {} partitions.", this.topic, controlTopicPartitions);
    }

    void destroy() {
        producer.close();
    }


    public void sendMessage(String message) {
        //int partition = Math.abs(kafkaMessage.getKafkaPartitionAttribute().hashCode() % controlTopicPartitions);
        ProducerRecord<String, String> data = new ProducerRecord<>(this.topic, null, message);
        producer.send(data, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {

            }
        });

        //boolean success = false;

        //asynchronous
        /*this.producer.send(data, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {

            }
        });*/

        //synchronous
        /*for (int retry = MAX_RETRIES; !success && retry > 0; retry--) {
            try {
                this.producer.send(data).get();
                success = true;
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted while trying to send the status message '" + message +
                        "' control topic. Giving up.");
                break;
            } catch (ExecutionException e) {
                LOGGER.error("Failed to send status message '" + message + "' to control topic. " +
                        "Retrying after " + RETRY_INTERVAL_MS + " ms.");
                try {
                    Thread.sleep(RETRY_INTERVAL_MS);
                } catch (InterruptedException e1) {
                    LOGGER.error("Interrupted while waiting to retry the sending of the status message '" +
                            message + "' to control topic. Giving up.");
                    break;
                }
            }
        }

        if (!success) {
            throw new RuntimeException("Failed to send status message '" + message + "' to control topic.");
        }*/
    }
}
