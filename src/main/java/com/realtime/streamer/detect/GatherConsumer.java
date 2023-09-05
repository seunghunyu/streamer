package com.realtime.streamer.detect;

import com.realtime.streamer.rebminterface.CoWorker;
import com.realtime.streamer.data.Camp;
import com.realtime.streamer.repository.rebm.JdbcTemplateCampRepository;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
/*
 * [2023.06.27] 신규 생성 - 감지 토픽에서 데이터 컨슈밍
 *
 *
 */
@Order(3)
@EnableAsync
@RequiredArgsConstructor
@Component
//public class GatherConsumer implements DataConsumer,ApplicationRunner {
public class GatherConsumer implements CoWorker, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "TEST";

    Properties consumerConfigs;
    Properties producerConfigs;

    KafkaConsumer<String, String> consumer;
    KafkaProducer<String, String> producer;

    int lastUpdate = 0;
    List<Camp> useDetcChanList;
    //신규 감지 ID
    BigDecimal newDetectId;


    @Autowired
    JdbcTemplateCampRepository repository;

    @Autowired
    CampService campService;

    @Autowired
    Utility utility;

    public GatherConsumer(String address, String groupId, String topic) {
        System.out.println("call Gather Consumer Constructor");
        this.Address = address;
        this.GroupId = groupId;
        this.topic = topic;
        this.lastUpdate = LocalTime.now().getSecond();
    }


    @Override
    public void polling(Consumer consumer, Producer producer){
        int SuccessCnt = 0;
        int FailCnt = 0;
        if(lastUpdate + 7 < LocalTime.now().getSecond()){
            useDetcChanList = campService.getDetcChanList();
        }
        //useDetcChanList = repository.getDetcChanList();
//        System.out.println("사용중인 감지채널 :::::" + useDetcChanList.get(0).getDetcChanCd());
        System.out.println("사용중인 감지채널 :::::" + "9001");

        try{
            loop:
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500)); //데이터가 없을 경우 최대 0.5초 기다림

                if(records.count() == 0) continue ;

                System.out.println("records count ::"+Integer.toString(records.count()));

//                if(FailCnt > 10){
//                    break loop;
//                }
//                if(records.isEmpty()){
//                    FailCnt++;
//                    continue;
//                }
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("GATHER CONSUMER1 @@@@@@@@@@@@ " + record.value());

                    JSONParser parser = new JSONParser();
//                    JSONObject bjob = (JSONObject)parser.parse(record.value());
                    JSONObject bjob = (JSONObject)parser.parse(record.value());
                    System.out.println("GATHER CONSUMER2 @@@@@@@@@@@@ " + bjob.toString());
                    System.out.println(bjob.get("CUST_ID"));

                    bjob.put("REBM_DETECT_ID" , String.valueOf(System.currentTimeMillis())+bjob.get("CUST_ID")); //감지아이디
                    bjob.put("WORK_DTM_MIL", String.valueOf(System.currentTimeMillis()));
                    bjob.put("WORK_SVR_ID","A");
                    bjob.put("WORK_SVR_NM","serverA");
//                    bjob.put("DETC_CHAN_CD", "9009"); // Kafka 채널 코드 임시
                    bjob.put("DETC_CHAN_CD", "9001"); // Kafka 채널 코드 임시
                    SuccessCnt++;
                    System.out.println("Success count : "+SuccessCnt);
//                    if (SuccessCnt >= 30) { //최대 500건 get
//                        consumer.commitSync(); //commit
//                        break loop; //탈출
//                    }
                    consumer.commitSync(); //commit
                    producing(producer, bjob.toString());
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


    public void producing(Producer producer, String producingData){

        String assignTopic   = "ASSIGN";
        String ruleTopic     = "RULE";
        String detcSaveTopic = "DETC_SAVE";

        int num = 0;

//        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(conf);;
        ProducerRecord<String, String> record, record2;
//        record = new ProducerRecord<>(ruleTopic, producingData);
        record = new ProducerRecord<>(assignTopic, producingData);
        record2 = new ProducerRecord<>(detcSaveTopic, producingData);

        try {
            //  Thread.sleep(2000);
//            System.out.println("RULE MESSAGE PRODUCING:::::: " + producingData);
//            //Rule처리로 이동하는 메시지
//            if(producer == null){
//                System.out.println("@@@@@@@@@@@@@@@@@@gather consumer producer is null");
//            }
//            producer.send(record, (metadata, exception) -> {
//                if (exception != null) {
//                    System.out.println("RULE TOPIC SENDING EXCEPTION :: "+ exception.toString());
//                }
//            });
            System.out.println("ASSIGN MESSAGE PRODUCING:::::: " + producingData);
            //Rule처리로 이동하는 메시지
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("ASSIGN TOPIC SENDING EXCEPTION :: "+ exception.toString());
                }
            });
            System.out.println("DETC SAVE PRODUCING:::::: " + producingData);
            //감지이력 저장으로 이동하는 메시지
            producer.send(record2, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("DETC SAVE TOPIC SENDING EXCEPTION ::" + exception.toString());
                }
            });
        } catch (Exception e) {
            System.out.println("PRODUCING EXCEPTION ::" + e.toString());
        } finally {
            producer.flush();
        }

    }

    @Async
    @Override
    public void run(String... args) throws Exception {
        System.out.println("Gather Consumer START::::::::::::::::::::::::::::::::::");
        GatherConsumer gatherConsumer = new GatherConsumer("192.168.20.57:9092","test-consumer-group","TEST");
        gatherConsumer.consumerConfigs = utility.setKafkaConsumerConfigs(gatherConsumer.Address, gatherConsumer.GroupId);
        gatherConsumer.producerConfigs = utility.setKafkaProducerConfigs(gatherConsumer.Address);

        gatherConsumer.consumer = new KafkaConsumer<String, String>(gatherConsumer.consumerConfigs);
        gatherConsumer.producer = new KafkaProducer<String, String>(gatherConsumer.producerConfigs);
        gatherConsumer.consumer.subscribe(Arrays.asList(gatherConsumer.topic));

        polling(gatherConsumer.consumer, gatherConsumer.producer);
    }
}
