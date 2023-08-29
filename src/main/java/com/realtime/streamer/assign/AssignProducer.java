package com.realtime.streamer.assign;

import com.realtime.streamer.Queue.AssignQueue;
import com.realtime.streamer.consumer.CoWorker;
import com.realtime.streamer.data.Camp;
import com.realtime.streamer.service.CampService;
import com.realtime.streamer.util.Utility;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.json.JsonParseException;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

@Order(5)
@EnableAsync
@RequiredArgsConstructor
@Component
public class AssignProducer implements CoWorker, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "TEST";

    Properties consumerConfigs;
    Properties producerConfigs;

    KafkaConsumer<String, String> consumer;
    KafkaProducer<String, String> producer;

    int lastUpdate = 0;

    String table_dt, toDate, toTime;
    int seqNo = 0;
    int campCnt = 0;

    java.text.DateFormat dateFormat1 = new java.text.SimpleDateFormat("yyyyMMdd");
    java.text.DateFormat dateFormat2 = new java.text.SimpleDateFormat("HHmm");
    java.text.DateFormat dfhhmmss = new java.text.SimpleDateFormat("HHmmss");

    HashMap<String,String> hashChanFlowId = new HashMap<String,String>();
    AssignQueue assignQueue = new AssignQueue();

    @Autowired
    CampService campService;

    @Autowired
    Utility utility;

    public AssignProducer(String address, String groupId, String topic) {
        System.out.println("call Assign Producer Constructor");
        this.Address = address;
        this.GroupId = groupId;
        this.topic = topic;
        this.lastUpdate = LocalTime.now().getSecond();
    }


    @Override
    public void polling(Consumer consumer, Producer producer){
        int SuccessCnt = 0;
        int FailCnt = 0;

        try{
            loop:
            while(true){
                if(lastUpdate + 7 < LocalTime.now().getSecond()){

                }

                if(assignQueue.getAssignProdQ().size() == 0){
                    continue;
                }else if(assignQueue.getAssignProdQ().size() > 0) {
                    String assignProdItem = assignQueue.getProdQueueItem();
                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject) parser.parse(assignProdItem);

                    System.out.println("AssingProducer !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + bjob.toString());


                    producing(producer, bjob.toString());
                }

            }
        }catch(JsonParseException e){
            System.out.println("JsonParsing Error:::" + e.getMessage());
            //e.printStackTrace();
        }catch(Exception e) {
            System.out.println("Exception:::" + e.getMessage());
        }finally{

        }

    }


    public void producing(Producer producer, String producingData){

        String ruleTopic = "RULE";
        String ruleFTopic = "RULEF_SAVE";

        int num = 0;

        ProducerRecord<String, String> record, record2;
        record = new ProducerRecord<>(ruleTopic, producingData);
        record2 = new ProducerRecord<>(ruleFTopic, producingData);

        try {
            //  Thread.sleep(2000);
            System.out.println("RULE MESSAGE PRODUCING:::::: " + producingData);
            //Rule처리로 이동하는 메시지
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("RULE TOPIC SENDING EXCEPTION :: "+ exception.toString());
                }
            });
            System.out.println("RULE FAIL SAVE PRODUCING:::::: " + producingData);
            //감지이력 저장으로 이동하는 메시지
            producer.send(record2, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("RULE FAIL SAVE TOPIC SENDING EXCEPTION ::" + exception.toString());
                }
            });
        } catch (Exception e) {
            System.out.println("Assgin Consumer PRODUCING EXCEPTION ::" + e.toString());
        } finally {
            producer.flush();
        }

    }

    @Async
    @Override
    public void run(String... args) throws Exception {
        System.out.println("Assign Producer START::::::::::::::::::::::::::::::::::");
        AssignProducer assignProducer = new AssignProducer("192.168.20.57:9092","test-consumer-group","ASSIGN");
        assignProducer.consumerConfigs = utility.setKafkaConsumerConfigs(assignProducer.Address, assignProducer.GroupId);
        assignProducer.producerConfigs = utility.setKafkaProducerConfigs(assignProducer.Address);

        assignProducer.consumer = new KafkaConsumer<String, String>(assignProducer.consumerConfigs);
        assignProducer.producer = new KafkaProducer<String, String>(assignProducer.producerConfigs);
        assignProducer.consumer.subscribe(Arrays.asList(assignProducer.topic));

        polling(assignProducer.consumer, assignProducer.producer);
    }
}
