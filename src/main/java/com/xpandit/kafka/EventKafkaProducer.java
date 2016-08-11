package com.xpandit.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

/**
 * Created by xpandit on 7/29/16.
 */
public class EventKafkaProducer {

    public static final String ZOOKEEPER = "localhost:2181";
    public static final String BROKERS = "localhost:9092";
    public static final String TOPIC = "events";


    public static void main(String[] args){


        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<>(props);

        try {

            List<String> eventLines = Files.readAllLines(Paths.get("src/main/resources/input/events.txt"));

            int currentEvent = 0;
            int nextStop = 10000;

            while(currentEvent < eventLines.size()) {

                while(currentEvent < nextStop) {
                    String msg = System.currentTimeMillis() + "|" + eventLines.get(currentEvent);

                    ProducerRecord<String,String> producerRecord = new ProducerRecord<>(TOPIC, null/*key*/, msg);
                    producer.send(producerRecord);          //async send
                    currentEvent++;
                }

                System.out.println("Injected 10000 events to Kafka");

                nextStop += 10000;

                Thread.sleep(5000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            producer.close();
        }
    }
}
