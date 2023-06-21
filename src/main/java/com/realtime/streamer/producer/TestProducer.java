package com.realtime.streamer.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class TestProducer {
    private static final String TOPIC_NAME = "TEST";
    private static final String FIN_MESSAGE = "exit";
    String IP = "192.168.20.57:9092";
    String topic = "TEST";
    Properties configs;

    public TestProducer(String IP, String topic, Properties configs) {
        this.IP = IP;
        this.topic = topic;
        this.configs = configs;
    }

    public void sendMessageToTopic(){
        Properties configs = new Properties();
        configs.put("bootstrap.servers", IP); // kafka host 및 server 설정
        configs.put("acks", "all");                         // 자신이 보낸 메시지에 대해 카프카로부터 확인을 기다리지 않습니다.
        configs.put("block.on.buffer.full", "true");        // 서버로 보낼 레코드를 버퍼링 할 때 사용할 수 있는 전체 메모리의 바이트수
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");   // serialize 설정
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // serialize 설정

        //고객 ID 생성
        String cust_id = Integer.toString((int)(Math.random() * 10000));

        // producer 생성
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);
        int num = 0;
        while(true) {
            cust_id = Integer.toString((int)(Math.random() * 10000));
            System.out.print("sendMessage > ");
            String message = "{'cust_id' : "+ cust_id+ ", 'cust_name' : 'Yu', 'age' : 33 }";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);

            try {
                Thread.sleep(3000);
                System.out.println(Integer.toString(num++)+"번째 메시지 :: " + message);
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
}
