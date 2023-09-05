package com.realtime.streamer.rule;

import com.realtime.streamer.Queue.RuleExQueue;
import com.realtime.streamer.rebminterface.CoWorker;
import com.realtime.streamer.rebminterface.DataProducer;
import com.realtime.streamer.service.CampService;
import com.realtime.streamer.util.Utility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.json.JsonParseException;

import java.time.LocalTime;
import java.util.HashMap;
import java.util.Properties;

public class RuleProducer implements DataProducer, CommandLineRunner {
    String Address = "192.168.20.57:9092";

    Properties producerConfigs;

    KafkaProducer<String, String> producer;

    int lastUpdate = 0;

    String table_dt, toDate, toTime;
    int seqNo = 0;
    int campCnt = 0;

    java.text.DateFormat dateFormat1 = new java.text.SimpleDateFormat("yyyyMMdd");
    java.text.DateFormat dateFormat2 = new java.text.SimpleDateFormat("HHmm");
    java.text.DateFormat dfhhmmss = new java.text.SimpleDateFormat("HHmmss");

    HashMap<String,String> hashChanFlowId = new HashMap<String,String>();
    RuleExQueue ruleExQueue = new RuleExQueue();

    @Autowired
    CampService campService;

    @Autowired
    Utility utility;

    public RuleProducer(String address) {
        System.out.println("call Rule Producer Constructor");
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

                if(ruleExQueue.getRuleProdQ().size() == 0){
                    continue;
                }else if(ruleExQueue.getRuleProdQ().size() > 0) {
                    String assignProdItem = ruleExQueue.getProdQueueItem();
                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject) parser.parse(assignProdItem);

                    System.out.println("RuleProducer !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + bjob.toString());

                    producing(producer, bjob.toString(), bjob.get("resultStr").toString());
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


//    public void producing(Producer producer, String producingData, String resultStr){
    public void producing(Producer producer, String producingData, String resultStr){
        String clanTopic = "CLAN";
        String ruleSTopic = "RULES_SAVE";
        String ruleFTopic = "RULEF_SAVE";
        int num = 0;


        ProducerRecord<String, String> clanRecord, ruleSRecord, ruleFRecord;
        clanRecord = new ProducerRecord<>(clanTopic, producingData);
        ruleSRecord = new ProducerRecord<>(ruleSTopic, producingData);
        ruleFRecord = new ProducerRecord<>(ruleFTopic, producingData);

        try {
            //1. Rule 성공 , Clan 으로 이동
            if(resultStr == "true") {
                System.out.println("RULE CONSUMER CLAN MESSAGE PRODUCING:::::: " + producingData);
                //Rule처리로 이동하는 메시지
                producer.send(clanRecord, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("RULE CONSUMER CLAN TOPIC SENDING EXCEPTION :: " + exception.toString());
                    }
                });
                System.out.println("RULE CONSUMER RULE SUCCESS SAVE PRODUCING:::::: " + producingData);
                //감지이력 저장으로 이동하는 메시지
                producer.send(ruleSRecord, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("RULE CONSUMER RULE SUCCESS TOPIC SENDING EXCEPTION ::" + exception.toString());
                    }
                });
                //2. Rule 실패
            }else {
                System.out.println("RULE CONSUMER RULE FAIL SAVE PRODUCING:::::: " + producingData);
                //감지이력 저장으로 이동하는 메시지
                producer.send(ruleFRecord, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("RULE CONSUMER RULE FAIL TOPIC SENDING EXCEPTION ::" + exception.toString());
                    }
                });
            }

        } catch (Exception e) {
            System.out.println("Rule Consumer PRODUCING EXCEPTION ::" + e.toString());
        } finally {
            producer.flush();
        }
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("RuleProducer START::::::::::::::::::::::::::::::::::");

        RuleProducer ruleProducer = new RuleProducer("192.168.20.57:9092");
        ruleProducer.producerConfigs = utility.setKafkaProducerConfigs(ruleProducer.Address);
        ruleProducer.producer = new KafkaProducer<String, String>(ruleProducer.producerConfigs);

        polling(ruleProducer.producer);
    }
}
