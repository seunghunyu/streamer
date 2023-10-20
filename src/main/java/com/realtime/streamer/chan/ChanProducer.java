package com.realtime.streamer.chan;

import com.realtime.streamer.Queue.AssignQueue;
import com.realtime.streamer.Queue.ChanExQueue;
import com.realtime.streamer.clan.ClanProducer;
import com.realtime.streamer.data.ChanEx;
import com.realtime.streamer.rebminterface.DataProducer;
import com.realtime.streamer.service.CampService;
import com.realtime.streamer.util.Utility;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

public class ChanProducer implements DataProducer, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "TEST";

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


    @Autowired
    CampService campService;

    @Autowired
    Utility utility;

    ChanExQueue chanExQueue = new ChanExQueue();

    public ChanProducer(String address) {
        System.out.println("call Chan Producer Constructor");
        this.Address = address;
        this.lastUpdate = LocalTime.now().getSecond();
    }

    @Override
    public void polling(Producer producer) {
        int SuccessCnt = 0;
        int FailCnt = 0;

        try{
            loop:
            while(true){
                if(lastUpdate + 7 < LocalTime.now().getSecond()){

                }

                if(chanExQueue.getChanProdQ().size() == 0){
                    continue;
                }else if(chanExQueue.getChanProdQ().size() > 0) {
                    String chanProdItem = chanExQueue.getProdQueueItem();
                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject) parser.parse(chanProdItem);

                    System.out.println("ChanProducer !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + bjob.toString());


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
            System.out.println("CHAN  PRODUCING EXCEPTION ::" + e.toString());
        } finally {
            producer.flush();
        }

    }



    @Override
    public void run(String... args) throws Exception {
        System.out.println("Chan Producer START::::::::::::::::::::::::::::::::::");
        ChanProducer chanProducer = new ChanProducer("192.168.20.57:9092");

        chanProducer.producerConfigs = utility.setKafkaProducerConfigs(chanProducer.Address);
        chanProducer.producer = new KafkaProducer<String, String>(chanProducer.producerConfigs);

        polling(chanProducer.producer);
    }
}
