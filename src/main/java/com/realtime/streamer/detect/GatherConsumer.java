package com.realtime.streamer.detect;

import com.realtime.streamer.cosumer.DataConsumer;
import com.realtime.streamer.data.Camp;
import com.realtime.streamer.repository.JdbcTemplateCampRepository;
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
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
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
import java.util.Map;
import java.util.Properties;
/*
 * [2023.06.27] 신규 생성 - 감지 토픽에서 데이터 컨슈밍
 *
 *
 */
@Order(2)
@EnableAsync
@RequiredArgsConstructor
@Component
//public class GatherConsumer implements DataConsumer,ApplicationRunner {
public class GatherConsumer implements DataConsumer, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "TEST";
    Properties configs;
    KafkaConsumer<String, String> consumer;
    String testVar = "";
    int lastUpdate = 0;
    List<Camp> useDetcChanList;
    //신규 감지 ID
    BigDecimal newDetectId;


    @Autowired
    JdbcTemplateCampRepository repository;


    public GatherConsumer(String address, String groupId, String topic) {
        System.out.println("call Gather Consumer Constructor");
        this.Address = address;
        this.GroupId = groupId;
        this.topic = topic;
        this.lastUpdate = LocalTime.now().getSecond();
        this.testVar = "qqqqq";

        this.configs = new Properties();
        this.configs.put("bootstrap.servers", Address); // kafka server host 및 port
        //192.168.20.99:9092,192.168.20.100:9092,192.168.20.101:9092
        this.configs.put("session.timeout.ms", "10000"); // session 설정
        this.configs.put("group.id", GroupId); // 그룹아이디 설정
        this.configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // key deserializer
        this.configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // value deserializer
        this.configs.put("auto.offset.reset", "latest"); // earliest(처음부터 읽음) | latest(현재부터 읽음)
        this.configs.put("enable.auto.commit", false); //AutoCommit 여부

        this.configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");   // serialize 설정
        this.configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // serialize 설정

        this.configs.put("acks", "all");                         // 자신이 보낸 메시지에 대해 카프카로부터 확인을 기다리지 않습니다.
        this.configs.put("block.on.buffer.full", "true");        // 서버로 보낼 레코드를 버퍼링 할 때 사용할 수 있는 전체 메모리의 바이트수

        this.consumer = new KafkaConsumer<String, String>(this.configs);
        this.consumer.subscribe(Arrays.asList(topic)); // 구독할 topic 설정


        System.out.println("######### consturctor info");
        System.out.println("%%"+configs.get("group.id"));
        System.out.println("%%"+configs.get("bootstrap.servers"));
        System.out.println("%%"+configs.get("group.id"));
        System.out.println("%%"+configs.get("auto.offset.reset"));
        System.out.println("######### consturctor info end");

    }

    public void polling(Properties conf, Consumer consumer){
        int SuccessCnt = 0;
        int FailCnt = 0;
        if(lastUpdate + 7 < LocalTime.now().getSecond()){
            useDetcChanList = repository.getDetcChanList();
        }
        useDetcChanList = repository.getDetcChanList();
//        System.out.println("사용중인 감지채널 :::::" + useDetcChanList.get(0).getDetcChanCd());
        System.out.println("사용중인 감지채널 :::::" + "9001");
//        System.out.println("@@@kafka config info@@@");
//        System.out.println("%%"+conf.get("bootstrap.servers"));
//        System.out.println("%%"+conf.get("group.id"));
//        System.out.println("%%"+conf.get("auto.offset.reset"));
//        System.out.println("#########"+testVar);
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
                    System.out.println("GATHER CONSUMER1 @@@@@@@@@@@@ " + record.value() + record);

                    JSONParser parser = new JSONParser();
//                    JSONObject bjob = (JSONObject)parser.parse(record.value());
                    JSONObject bjob = (JSONObject)parser.parse(record.value());
                    System.out.println("GATHER CONSUMER2 @@@@@@@@@@@@ " + bjob.toString());

                    bjob.put("REBM_DETECT_ID" , String.valueOf(System.currentTimeMillis())+bjob.get("CUST_ID")); //감지아이디
                    bjob.put("WORK_DTM_MIL", String.valueOf(System.currentTimeMillis()));
                    bjob.put("WORK_SVR_ID","A");
                    bjob.put("WORK_SVR_NM","serverA");
                    bjob.put("SIDO_CD", "001");

                    String na = "";
                    System.out.println("@@ "+ System.currentTimeMillis());

                    for (Object e : bjob.entrySet())  // 아이템화 시작
                    {
                        Map.Entry entry = (Map.Entry) e;
                        na = String.valueOf(entry.getKey());
//                        tmpWorkInfo.hashmap.put(na, bjob.get(na).toString()); // (아이템명, 아이템값) 형태로 워커객체에 put

                        // Content에 CUST_ID 이름이 다른경우
                        // tmpWorkInfo.hashmap.put("CUST_ID", na.equals("고객번호에 해당하는 아이템")); 형태로 사용가능
                    }
//                    objectQueue.addWorkItem(tmpWorkInfo); // worker에 전달
//                    svrBridge.info_println("kafka message info : "+ tmpWorkInfo);
                    SuccessCnt++;
                    System.out.println("Success count : "+SuccessCnt);
//                    if (SuccessCnt >= 30) { //최대 500건 get
//                        consumer.commitSync(); //commit
//                        break loop; //탈출
//                    }
                    consumer.commitSync(); //commit
                    producing(conf, bjob.toString());
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


    public void producing(Properties conf, String producingData){
        System.out.println("producing1");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(conf);
        System.out.println("producing2");
        ProducerRecord<String, String> record, record2;
        String ruleTopic = "RULE";
        String detcSaveTopic = "DETC_SAVE";
        int num = 0;

//        while(true) {
        //System.out.print("sendMessage > " + producingData);
        record = new ProducerRecord<>(ruleTopic, producingData);
        record2 = new ProducerRecord<>(detcSaveTopic, producingData);

        try {
            //  Thread.sleep(2000);
            System.out.println("RULE ::::"+Integer.toString(num++)+"번째 메시지 :: " + producingData);
            //Rule처리로 이동하는 메시지
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("RULE TOPIC SENDING EXCEPTION :: "+ exception.toString());
                }
            });
            System.out.println("DETC SAVE ::::"+Integer.toString(num++)+"번째 메시지 :: " + producingData);
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
//        }

    }

//    @Override
//    public void run(ApplicationArguments args) throws Exception {
//        System.out.println("Gather Consumer START::::::::::::::::::::::::::::::::::");
//        GatherConsumer gatherConsumer = new GatherConsumer("192.168.20.57:9092","test-consumer-group","TEST");
//        polling(gatherConsumer.configs, gatherConsumer.consumer);
//
//    }
    @Async
    @Override
    public void run(String... args) throws Exception {
        System.out.println("Gather Consumer START::::::::::::::::::::::::::::::::::");
        GatherConsumer gatherConsumer = new GatherConsumer("192.168.20.57:9092","test-consumer-group","TEST");
        polling(gatherConsumer.configs, gatherConsumer.consumer);
    }
}
