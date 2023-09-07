package com.realtime.streamer.clan;

import com.realtime.streamer.Queue.AssignQueue;
import com.realtime.streamer.assign.AssignProducer;
import com.realtime.streamer.rebminterface.DataProducer;
import com.realtime.streamer.service.CampService;
import com.realtime.streamer.util.Utility;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.json.JsonParseException;
import org.springframework.scheduling.annotation.Async;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

public class ClanProducer implements DataProducer, CommandLineRunner {
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

    public ClanProducer(String address) {
        System.out.println("call Assign Producer Constructor");
        this.Address = address;
        this.lastUpdate = LocalTime.now().getSecond();
    }


    @Override
    public void polling(Producer producer){
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


                    producing(bjob.toString(), Boolean.parseBoolean(bjob.get("notClean").toString()));
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

    public void producing(String producingData, boolean notClean){
        //채널 발송 전 단계에서의 처리 토픽
        String chanTopic = "CHAN";
        //중복제거에 걸린 데이터 저장
        String clanSaveTopic = "CLAN_SAVE";

        int num = 0;

        ProducerRecord<String, String> record, record2;
        record = new ProducerRecord<>(chanTopic, producingData);
        record2 = new ProducerRecord<>(clanSaveTopic, producingData);

        try {

            //중복제거 대상이 아닌 경우
            if(!notClean) {
                System.out.println("CHAN MESSAGE PRODUCING:::::: " + producingData);
                //채널 전송으로 이동하는 메시지
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("CHAN TOPIC SENDING EXCEPTION :: " + exception.toString());
                    }
                });
                //중복제거 대상인 경우
            }else {
                System.out.println("CLAN SAVE PRODUCING:::::: " + producingData);
                //중복제거 이력 저장으로 이동하는 메시지

                producer.send(record2, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("CLAN SAVE TOPIC SENDING EXCEPTION ::" + exception.toString());
                    }
                });
            }
        } catch (Exception e) {
            System.out.println("Clan Consumer PRODUCING EXCEPTION ::" + e.toString());
        } finally {
            producer.flush();
        }

    }

    @Async
    @Override
    public void run(String... args) throws Exception {
        System.out.println("Clan Producer START::::::::::::::::::::::::::::::::::");
        ClanProducer clanProducer = new ClanProducer("192.168.20.57:9092");

        clanProducer.producerConfigs = utility.setKafkaProducerConfigs(clanProducer.Address);
        clanProducer.producer = new KafkaProducer<String, String>(clanProducer.producerConfigs);

        polling(clanProducer.producer);
    }

}
