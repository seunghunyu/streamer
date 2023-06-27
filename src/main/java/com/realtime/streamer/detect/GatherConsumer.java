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
import org.springframework.boot.json.JsonParseException;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
/*
 * [2023.06.27] �ű� ���� - ���� ���ȿ��� ������ ������
 *
 *
 */
@RequiredArgsConstructor
@Component
public class GatherConsumer implements DataConsumer,ApplicationRunner {
    String Address = "192.168.20.77:9092";
    String GroupId = "test-consumer-group";
    String topic   = "TEST";
    Properties configs;
    KafkaConsumer<String, String> consumer;
    String testVar = "";
    int lastUpdate = 0;
    List<Camp> useDetcChanList;
    //�ű� ���� ID
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
        this.configs.put("bootstrap.servers", Address); // kafka server host �� port
        //192.168.20.99:9092,192.168.20.100:9092,192.168.20.101:9092
        this.configs.put("session.timeout.ms", "10000"); // session ����
        this.configs.put("group.id", GroupId); // �׷���̵� ����
        this.configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // key deserializer
        this.configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // value deserializer
        this.configs.put("auto.offset.reset", "latest"); // earliest(ó������ ����) | latest(������� ����)
        this.configs.put("enable.auto.commit", false); //AutoCommit ����

        this.configs.put("acks", "all");                         // �ڽ��� ���� �޽����� ���� ī��ī�κ��� Ȯ���� ��ٸ��� �ʽ��ϴ�.
        this.configs.put("block.on.buffer.full", "true");        // ������ ���� ���ڵ带 ���۸� �� �� ����� �� �ִ� ��ü �޸��� ����Ʈ��

        this.consumer = new KafkaConsumer<String, String>(this.configs);
        this.consumer.subscribe(Arrays.asList(topic)); // ������ topic ����


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
//        System.out.println("������� ����ä�� :::::" + useDetcChanList.get(0).getDetcChanCd());
        System.out.println("������� ����ä�� :::::" + "9001");
        System.out.println("@@@kafka config info@@@");
        System.out.println("%%"+conf.get("bootstrap.servers"));
        System.out.println("%%"+conf.get("group.id"));
        System.out.println("%%"+conf.get("auto.offset.reset"));
        System.out.println("#########"+testVar);
        try{
            loop:
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500)); //�����Ͱ� ���� ��� �ִ� 0.5�� ��ٸ�

                if(records.count() == 0) continue ;

                System.out.println("records :::" + records + ":::count ::"+Integer.toString(records.count()));

//                if(FailCnt > 10){
//                    break loop;
//                }
//                if(records.isEmpty()){
//                    FailCnt++;
//                    continue;
//                }
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("kafka message ::::" + record.value());

                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(record.value());

                    bjob.put("REBM_DETECT_ID" , String.valueOf(System.currentTimeMillis())+bjob.get("CUST_ID")); //�������̵�
                    bjob.put("WORK_DTM_MIL", String.valueOf(System.currentTimeMillis()));
                    bjob.put("WORK_SVR_ID","A");
                    bjob.put("WORK_SVR_NM","serverA");
                    bjob.put("SIDO_CD", "001");

                    String na = "";
                    System.out.println("@@ "+ System.currentTimeMillis());

                    for (Object e : bjob.entrySet())  // ������ȭ ����
                    {
                        Map.Entry entry = (Map.Entry) e;
                        na = String.valueOf(entry.getKey());
//                        tmpWorkInfo.hashmap.put(na, bjob.get(na).toString()); // (�����۸�, �����۰�) ���·� ��Ŀ��ü�� put

                        // Content�� CUST_ID �̸��� �ٸ����
                        // tmpWorkInfo.hashmap.put("CUST_ID", na.equals("����ȣ�� �ش��ϴ� ������")); ���·� ��밡��
                    }
//                    objectQueue.addWorkItem(tmpWorkInfo); // worker�� ����
//                    svrBridge.info_println("kafka message info : "+ tmpWorkInfo);
                    SuccessCnt++;
                    System.out.println("Success count : "+SuccessCnt);
//                    if (SuccessCnt >= 30) { //�ִ� 500�� get
//                        consumer.commitSync(); //commit
//                        break loop; //Ż��
//                    }
                    consumer.commitSync(); //commit
                    producing(bjob.toString());
                }
                consumer.commitSync();//commit
            }
        }catch(JsonParseException e){
            System.out.println(e.getMessage());
            //e.printStackTrace();
        }catch(Exception e) {


        }finally{

        }

    }


    public void producing(String producingData){
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);
        ProducerRecord<String, String> record, record2;
        String ruleTopic = "RULE";
        String detcSaveTopic = "DETC_SAVE";
        int num = 0;

        while(true) {
            System.out.print("sendMessage > " + producingData);
            record = new ProducerRecord<>(ruleTopic, producingData);
            record2 = new ProducerRecord<>(detcSaveTopic, producingData);

            try {
                Thread.sleep(2000);
                System.out.println("Rule ::::"+Integer.toString(num++)+"��° �޽��� :: " + producingData);
                //Ruleó���� �̵��ϴ� �޽���
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println(exception.toString());
                    }
                });
                //�����̷� �������� �̵��ϴ� �޽���
                producer.send(record2, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println(exception.toString());
                    }
                });
            } catch (Exception e) {
                // exception
            } finally {
                producer.flush();
            }
        }

    }

    @Override
    public void run(ApplicationArguments args) throws Exception {

        GatherConsumer gatherConsumer = new GatherConsumer("192.168.20.57:9092","test-consumer-group","TEST");
        polling(gatherConsumer.configs, gatherConsumer.consumer);

    }
}
