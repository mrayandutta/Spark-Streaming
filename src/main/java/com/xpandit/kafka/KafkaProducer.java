package com.xpandit.kafka;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by xpandit on 7/29/16.
 */
public class KafkaProducer {

    public static final String ZOOKEEPER = "localhost:2181";
    public static final String BROKERS = "localhost:9092";
    public static final String TOPIC = "events";


    public static void main(String[] args){

        File eventsFile = new File("src/main/resources/events");

        try {

            List<String> eventLines = Files.readAllLines(Paths.get(eventsFile.getPath()));

            KafkaProducerService kafkaProducerService = new KafkaProducerService(ZOOKEEPER, BROKERS, TOPIC);

            for (String event: eventLines){
                kafkaProducerService.sendMessage(event);
            }

            kafkaProducerService.destroy();
            System.out.println("Added " + eventLines.size() + "events to Kafka");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
