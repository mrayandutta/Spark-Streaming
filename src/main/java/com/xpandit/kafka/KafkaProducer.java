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

        File eventsFile = new File("src/main/resources/events.txt");

        KafkaProducerService kafkaProducerService = new KafkaProducerService(ZOOKEEPER, BROKERS, TOPIC);

        try {

            List<String> eventLines = Files.readAllLines(Paths.get(eventsFile.getPath()));

            int currentEvent = 0;
            int nextStop = 10000;

            while(currentEvent < eventLines.size()) {

                while(currentEvent < nextStop) {
                    String msg = System.currentTimeMillis() + "|" + eventLines.get(currentEvent);
                    kafkaProducerService.sendMessage(msg);
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
            kafkaProducerService.destroy();
        }

    }
}
