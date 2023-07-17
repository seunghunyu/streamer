package com.realtime.streamer.rule;

import com.realtime.streamer.cosumer.DataConsumer;
import com.realtime.streamer.detect.GatherConsumer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.json.JsonParseException;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/* [2023.07.04] 룰 처리 컨슈머
 *
 */
@Order(4)
@EnableAsync
@RequiredArgsConstructor
@Component
public class RuleConsumer implements DataConsumer, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "RULE";
    Properties configs;
    KafkaConsumer<String, String> consumer;
    int lastUpdate = 0;
    public static class InnerRuleWorkInfo
    {
        String            real_flow_id;
//        WorkInfo          workinfo;
        String            resultStr;
        CountDownLatch countDownLatch;
        long              starttime;
        long              elapsedtime;
        String            detc_route_id;
    }

    private BlockingQueue<InnerRuleWorkInfo> InnerRuleWorkInfoQueue = new LinkedBlockingQueue<InnerRuleWorkInfo>();

    public RuleConsumer(String address, String groupId, String topic) {
        this.Address = address;
        this.GroupId = groupId;
        this.topic = topic;
        this.lastUpdate = LocalTime.now().getSecond();

        this.configs = new Properties();
        this.configs.put("bootstrap.servers", Address);
        this.configs.put("session.timeout.ms", "10000"); // session 설정
        this.configs.put("group.id", GroupId); // 그룹아이디 설정
        this.configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //Producing을 위해 serializer 추가
        this.configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.configs.put("auto.offset.reset", "latest");
        this.configs.put("enable.auto.commit", false);
        this.configs.put("acks", "all");
        this.configs.put("block.on.buffer.full", "true");

        this.consumer = new KafkaConsumer<String, String>(this.configs);
        this.consumer.subscribe(Arrays.asList(topic)); // 구독할 topic 설정
    }

    @Override
    public void polling(Properties conf, Consumer consumer) {
        int SuccessCnt = 0;
        int FailCnt = 0;
        if(lastUpdate + 7 < LocalTime.now().getSecond()){
            //logic
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

                    //@@@@@@@@@@@@@@@@@@@@@@@RULE 수행 로직@@@@@@@@@@@@@@@@@@@@@@@@@@@

                    //@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
                    SuccessCnt++;
                    System.out.println("Success count : "+SuccessCnt + ", Fail count : "+ FailCnt);
                    if (SuccessCnt >= 500) { //최대 500건 get
                        consumer.commitSync(); //commit
                        SuccessCnt = 0;
                        FailCnt = 0;
                    }
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

        String clanTopic = "CLAN";
        String ruleSTopic = "RULES_SAVE";
        String ruleFTopic = "RULEF_SAVE";
        int num = 0;

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(conf);;
        ProducerRecord<String, String> clanRecord, ruleSRecord, ruleFRecord;
        clanRecord = new ProducerRecord<>(clanTopic, producingData);
        ruleSRecord = new ProducerRecord<>(ruleSTopic, producingData);
        ruleFRecord = new ProducerRecord<>(ruleFTopic, producingData);

        try {
            //  Thread.sleep(2000);
            System.out.println("CLAN MESSAGE PRODUCING:::::: " + producingData);
            //Rule처리로 이동하는 메시지
            producer.send(clanRecord, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("CLAN TOPIC SENDING EXCEPTION :: "+ exception.toString());
                }
            });
            System.out.println("RULE SUCCESS SAVE PRODUCING:::::: " + producingData);
            //감지이력 저장으로 이동하는 메시지
            producer.send(ruleSRecord, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("RULE SUCCESS TOPIC SENDING EXCEPTION ::" + exception.toString());
                }
            });

            System.out.println("RULE FAIL SAVE PRODUCING:::::: " + producingData);
            //감지이력 저장으로 이동하는 메시지
            producer.send(ruleFRecord, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("RULE FAIL TOPIC SENDING EXCEPTION ::" + exception.toString());
                }
            });

        } catch (Exception e) {
            System.out.println("PRODUCING EXCEPTION ::" + e.toString());
        } finally {
            producer.flush();
        }
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("RuleConsumer start ::::::::::::::::::::::::::::");
        RuleConsumer ruleConsumer = new RuleConsumer("192.168.20.57:9092","test-consumer-group","RULE");
        polling(ruleConsumer.configs, ruleConsumer.consumer);
    }
}
