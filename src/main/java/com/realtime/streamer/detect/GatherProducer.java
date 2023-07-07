package com.realtime.streamer.detect;

import com.realtime.streamer.cosumer.DataConsumer;
import com.realtime.streamer.producer.DataProducer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

/*
*[2023.06.27] ������ �߻��� Producer �ű� ����(������ �̿�� ApplicationRunner ����)
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

    //String address, String groupId, String topic
    public GatherProducer(String IP, String topic, Properties configs) {
        this.IP = IP;
        this.topic = topic;
        this.configs = configs;
    }
    @Override
    public void sendMessage(Properties configs, Producer producer) {
        configs = new Properties();
        configs.put("bootstrap.servers", IP); // kafka host �� server ����
        configs.put("acks", "all");                         // �ڽ��� ���� �޽����� ���� ī��ī�κ��� Ȯ���� ��ٸ��� �ʽ��ϴ�.
        configs.put("block.on.buffer.full", "true");        // ������ ���� ���ڵ带 ���۸� �� �� ����� �� �ִ� ��ü �޸��� ����Ʈ��
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");   // serialize ����
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // serialize ����

        //�� ID ����
        String cust_id = Integer.toString((int)(Math.random() * 10000));
        String age     = Integer.toString((int)(Math.random() * 10));
        // producer ����
        producer = new KafkaProducer<String, String>(configs);
        int num = 0;
        while(true) {
            cust_id = Integer.toString((int)(Math.random() * 10000));
            age     = Integer.toString((int)(Math.random() * 10));
            String message = "{\"CUST_ID\" : \""+ cust_id + "\", \"CUST_NAME\" : \"Yu\", \"AGE\" : "+ age +"}";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
            //System.out.println(topic);
            try {
                Thread.sleep(3000);
                System.out.println("GATHER PRODUCER @@@@@" + Integer.toString(num++)+"��° �޽��� :: " + message);
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
        GatherProducer gatherProducer = new GatherProducer("192.168.20.57:9092","TEST",configs);
        sendMessage(gatherProducer.configs, gatherProducer.producer);
    }
}
