package com.realtime.streamer.chan;

import com.realtime.streamer.Queue.ChanExQueue;
import com.realtime.streamer.rebminterface.DataConsumer;
import com.realtime.streamer.data.Camp;
import com.realtime.streamer.data.Olapp;
import com.realtime.streamer.service.CampService;
import com.realtime.streamer.service.ChanService;
import com.realtime.streamer.service.OlappService;
import com.realtime.streamer.util.Utility;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.json.JsonParseException;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;


@EnableAsync
@RequiredArgsConstructor
//@Component
//public class GatherConsumer implements DataConsumer,ApplicationRunner {
public class ChanConsumer implements DataConsumer, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "REALCHAN";

    Properties consumerConfigs, producerConfigs;

    KafkaConsumer<String, String> consumer;

    int lastUpdate = 0;

    ChanExQueue chanExQueue = new ChanExQueue();

    String tableDt = "";

    @Autowired
    Utility utility;

    public ChanConsumer(String address, String groupId, String topic) {
        System.out.println("call Gather Consumer Constructor");
        this.Address = address;
        this.GroupId = groupId;
        this.topic = topic;
        this.lastUpdate = LocalTime.now().getSecond();
    }

    public void polling(Consumer consumer){
        int SuccessCnt = 0;

        if(lastUpdate + 30 < LocalTime.now().getSecond()){

        }

        try{
            loop:
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500)); //데이터가 없을 경우 최대 0.5초 기다림

                if(records.count() == 0) continue ;
                System.out.println("records count ::"+Integer.toString(records.count()));

                for (ConsumerRecord<String, String> record : records) {
                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(record.value());

                    SuccessCnt++;
                    System.out.println("Clan Success count : "+SuccessCnt);

                    chanExQueue.addWorkQueueItem(bjob.toString());
                    consumer.commitSync(); //commit

                }
                consumer.commitSync();//commit
            }
        }catch(JsonParseException e){
            System.out.println("JsonParsing Error:::" + e.getMessage());
            //e.printStackTrace();
        }catch(Exception e) {
            System.out.println("Exception:::" + e.getMessage());
        }finally{

        }

    }




    @Async
    @Override
    public void run(String... args) throws Exception {
        System.out.println("ChanConsumer Consumer START::::::::::::::::::::::::::::::::::");
        ChanConsumer chanConsumer = new ChanConsumer("192.168.20.57:9092","test-consumer-group","REALCHAN");

        chanConsumer.consumerConfigs = utility.setKafkaConsumerConfigs(chanConsumer.Address, chanConsumer.GroupId);
        chanConsumer.consumer = new KafkaConsumer<String, String>(chanConsumer.consumerConfigs);
        chanConsumer.consumer.subscribe(Arrays.asList(chanConsumer.topic)); // 구독할 topic 설정


        chanConsumer.tableDt = utility.getTableDtNum();
        polling(chanConsumer.consumer);
    }
}
