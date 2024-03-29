package com.realtime.streamer.detect;

import com.realtime.streamer.producer.DataProducer;
import com.realtime.streamer.util.Utility;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

import java.util.Properties;

/*
*[2023.06.27] 데이터 발생용 Producer 신규 생성(실제로 이용시 ApplicationRunner 제거)
*
*
 */
@Order(2)
@EnableAsync
@RequiredArgsConstructor
@Component
//public class GatherProducer implements DataProducer, ApplicationRunner {
public class GatherProducer implements DataProducer, CommandLineRunner {
    private static final String TOPIC_NAME = "TEST";
    private static final String FIN_MESSAGE = "exit";
    String IP = "192.168.20.57:9092";
    String topic = "TEST";
    Properties configs;
    KafkaProducer<String, String> producer;

    @Autowired
    Utility utility;

    //String address, String groupId, String topic
    public GatherProducer(String IP, String topic) {
        this.IP = IP;
        this.topic = topic;
    }
    @Override
    public void sendMessage(Producer producer) {

        //고객 ID 생성
        String cust_id = Integer.toString((int)(Math.random() * 10000));
        String age     = Integer.toString((int)(Math.random() * 10));
//        // producer 생성
//        producer = new KafkaProducer<String, String>(configs);
        int num = 0;
        while(true) {
            cust_id = Integer.toString((int)(Math.random() * 10000));
            age     = Integer.toString((int)(Math.random() * 10));
            String message = "{\"CUST_ID\" : \""+ cust_id + "\", \"CUST_NAME\" : \"Yu\", \"AGE\" : "+ age +"}";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
            //System.out.println(topic);
            try {
                Thread.sleep(3000);
                System.out.println("GATHER PRODUCER @@@@@" + Integer.toString(num++)+"번째 메시지 :: " + message);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        // some exception
                        System.out.println(exception.toString());
                    }
                });

            } catch (Exception e) {
                // exception
            } finally {
                producer.flush();
            }

            if(message.equals(FIN_MESSAGE)) {
                producer.close();
                break;
            }
        }
    }

//    @Override
//    public void run(ApplicationArguments args) throws Exception {
//        System.out.println("Gather Producer START::::::::::::::::::::::::::::::::::");
//        GatherProducer gatherProducer = new GatherProducer("192.168.20.57:9092","TEST",configs);
//        sendMessage(gatherProducer.configs, gatherProducer.producer);
//    }

    @Async
    @Override
    public void run(String... args) throws Exception {
        System.out.println("Gather Producer START::::::::::::::::::::::::::::::::::");
        GatherProducer gatherProducer = new GatherProducer("192.168.20.57:9092","TEST");
        gatherProducer.configs = utility.setKafkaProducerConfigs(gatherProducer.IP);
        gatherProducer.producer = new KafkaProducer<String, String>(gatherProducer.configs);
        sendMessage(gatherProducer.producer);
    }
}
