package com.realtime.streamer.detect;

import com.realtime.streamer.cosumer.DataConsumer;
import com.realtime.streamer.util.Utility;
import jdk.jshell.execution.Util;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

import java.time.Duration;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

@Order(4)
@EnableAsync
@RequiredArgsConstructor
@Component
//public class GatherSaveConsumer implements DataConsumer, ApplicationRunner {
public class GatherSaveConsumer implements DataConsumer, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "detc-save-group";
    String topic   = "DETC_SAVE";
    Properties configs;
    KafkaConsumer<String, String> consumer;
    int lastUpdate = 0;

    @Autowired
    Utility utility;

    String tableDt;
    //������� ����ä�� ����Ʈ
    String DETC_CHAN_CD_LIST = "";
    //�������̺� ���� ����
    String INSQ_QRY = "";
    //�������̺� ���� ������
    String DETC_INST_ITEM = "";

    public GatherSaveConsumer(String address, String groupId, String topic) {
        this.Address = address;
        this.GroupId = groupId;
        this.topic = topic;
        this.lastUpdate = LocalTime.now().getSecond();

        this.configs = new Properties();
        this.configs.put("bootstrap.servers", Address); // kafka server host �� port
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
        //���Ϻ� ���̺� ��¥ ��ȸ
        this.tableDt = utility.getTableDtNum();

    }
    @Override
    public void polling(Properties conf, Consumer consumer) {
            int SuccessCnt = 0;
            int FailCnt = 0;
            String inst_Qry = "";
            try{
                loop:
                while(true){

                    if(lastUpdate + 60 < LocalTime.now().getSecond()){
                        //������� ����ä�� ��ȸ, �������̺� ���� ���� ��ȸ, ���� ���̺� ���� ������ ��ȸ
                        tableDt = utility.getTableDtNum();
                    }

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500)); //�����Ͱ� ���� ��� �ִ� 0.5�� ��ٸ�

                    if(records.count() == 0) continue ;

                    System.out.println("records :::" + records + ":::count ::"+Integer.toString(records.count()));

                    for (ConsumerRecord<String, String> record : records) {

                        System.out.println("DETC SAVE  ::::" + record.value());

                        JSONParser parser = new JSONParser();
                        JSONObject bjob = (JSONObject)parser.parse(record.value());
                        String detcChanCd = "";
                        //����ä�� key �� ����ÿ��� ������ ����
                        if(bjob.get("DETC_CHAN_CD") != null && !bjob.get("DETC_CHAN_CD").equals("")){
                            detcChanCd = bjob.get("DETC_CHAN_CD").toString();
                        }else{
                            inst_Qry = "";
                            continue;
                        }
                        //����ä�ο� �ش��ϴ� Insert �� ��������(Redis ���� ��������)
                        inst_Qry = utility.getRedisDetcChanInstSqlList(detcChanCd);
                        //


                        String na = "";
                        System.out.println("@@ "+ System.currentTimeMillis());


                        for (Object e : bjob.entrySet())  // ������ȭ ����
                        {
                            Map.Entry entry = (Map.Entry) e;
                            na = String.valueOf(entry.getKey());
                            bjob.get(na).toString();
                        }


                        SuccessCnt++;
                        if (SuccessCnt >= 30) { //�ִ� 500�� get
                            System.out.println("DETC_SAVE Success count : "+SuccessCnt);
                            consumer.commitSync(); //commit
                            SuccessCnt = 0;
                            //break loop; //Ż��
                        }
                        //consumer.commitSync(); //commit
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

//    @Override
//    public void run(ApplicationArguments args) throws Exception {
//        System.out.println("Gather SaveConsumer START::::::::::::::::::::::::::::::::::");
//        GatherSaveConsumer gatherSaveConsumer = new GatherSaveConsumer("192.168.20.57:9092", "detc-save-group", "DETC_SAVE");
//        polling(gatherSaveConsumer.configs, gatherSaveConsumer.consumer);
//
//    }
    @Async
    @Override
    public void run(String... args) throws Exception {
        System.out.println("Gather SaveConsumer START::::::::::::::::::::::::::::::::::");
        GatherSaveConsumer gatherSaveConsumer = new GatherSaveConsumer("192.168.20.57:9092", "detc-save-group", "DETC_SAVE");
        polling(gatherSaveConsumer.configs, gatherSaveConsumer.consumer);
    }
}
