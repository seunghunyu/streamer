package com.realtime.streamer.rule;

import com.realtime.streamer.cosumer.DataConsumer;
import com.realtime.streamer.detect.GatherConsumer;
import com.realtime.streamer.util.Utility;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.json.JsonParseException;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/* [2023.07.04] �� ó�� ������
 *
 */
@Order(4)
@EnableAsync
@RequiredArgsConstructor
//@Component
public class RuleConsumer implements DataConsumer, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "RULE";
    Properties cosumerConfigs, producerConfigs;
    KafkaConsumer<String, String> consumer;
    KafkaProducer<String, String> producer;
    int lastUpdate = 0;
    private Vector<InnerRuleWorkInfo> innerRuleWorkInfoArray =  new Vector<InnerRuleWorkInfo>();

    private Thread[] innerRuleWorkThread;
    private int      innerRuleWorkThreadCount = 0;;
    boolean isClose = false;
    int multiExecCount = 5;  //�ѹ��� ���� �� �� ���� �������� ����

    @Autowired
    Utility utility;

    public static class InnerRuleWorkInfo
    {
        String            real_flow_id;
//        WorkInfo          workinfo;
        JSONObject        workinfo;
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

        this.cosumerConfigs = utility.setKafkaConsumerConfigs(this.Address, this.GroupId);
        this.consumer = new KafkaConsumer<String, String>(this.cosumerConfigs);
        this.consumer.subscribe(Arrays.asList(topic)); // ������ topic ����

        this.producerConfigs = utility.setKafkaProducerConfigs(this.Address);
        this.producer = new KafkaProducer<String, String >(this.producerConfigs);
    }

    @Override
    public void polling(Consumer consumer) {
        int SuccessCnt = 0;
        int FailCnt = 0;
        if(lastUpdate + 7 < LocalTime.now().getSecond()){
            //logic
        }

        try{
            loop:
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500)); //�����Ͱ� ���� ��� �ִ� 0.5�� ��ٸ�

                if(records.count() == 0) continue ;

                System.out.println("records count ::"+Integer.toString(records.count()));

                for (ConsumerRecord<String, String> record : records) {
                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(record.value());

                    String routeIds = bjob.get("DETC_ROUTE_IDS").toString();
                    String flowIds = bjob.get("REAL_FLOW_IDS").toString();
                    String campIds = bjob.get("CAMP_IDS").toString();

                    String resultStr = "";

                    String[] arr_route_id = routeIds.split(",");
                    String[] arr_flow_id = flowIds.split(",");
                    String[] arr_camp_id = campIds.split(",");
                    String work_camp_id = "";
                    String work_route_id = "";
                    String work_flow_id = "";

                    InnerRuleWorkInfo innerRuleWorkInfo = null;
                    innerRuleWorkInfo.workinfo = bjob;

                    JSONArray arrExActInfo = new JSONArray();

                    innerRuleWorkInfoArray.clear();

                    //CountDownLatch �̿��Ͽ� �Ѿ�� ķ���ο� �ش��ϴ� �����ٷ��� Rule���� ������� ������ �� ķ������ �ش��ϴ� �� ��������� ������ᰡ �Ǹ� ������ ���� ����
                    CountDownLatch countDownLatch = new CountDownLatch(arr_route_id.length);
                    for(int i=0; i<arr_route_id.length; i++)
                    {
                        work_route_id = arr_route_id[i];
                        work_flow_id = arr_flow_id[i];
                        work_camp_id = arr_camp_id[i];    //work_route_id.substring(0, work_route_id.indexOf("_"));

                        if(work_camp_id != null && work_camp_id.length() > 3)
                        {
                            innerRuleWorkInfo = new InnerRuleWorkInfo();

                            //Rule ó���� �� ���� : �ΰ��̻��� �̺�Ʈ�� �������� ķ������ ���νý� ������ ���� ����� ���� ������ ����

//                            innerRuleWorkInfo.workinfo.hashmap.put("CAMP_ID", work_camp_id);
//                            innerRuleWorkInfo.workinfo.hashmap.put("EX_CAMP_ID", hashFlowId_ExCampId.get(work_flow_id));  
//                            innerRuleWorkInfo.workinfo.hashmap.put("OBZ_TIME_SEC", svrBridge.getTimeSec()+""); 
//                            innerRuleWorkInfo.workinfo.hashmap.put("WORK_SVR_ID", worksvrid);
//                            innerRuleWorkInfo.workinfo.hashmap.put("REAL_FLOW_ID", work_flow_id);

                            innerRuleWorkInfo.real_flow_id = work_flow_id;
                            innerRuleWorkInfo.detc_route_id = work_route_id;
                            innerRuleWorkInfo.countDownLatch = countDownLatch;
                            InnerRuleWorkInfoQueue.put(innerRuleWorkInfo);
                            innerRuleWorkInfoArray.addElement(innerRuleWorkInfo);
                        }
                    }
                    countDownLatch.await(); // �۾� ȣ��


//                    //@@@@@@@@@@@@@@@@@@@@@@@RULE ���� ����@@@@@@@@@@@@@@@@@@@@@@@@@@@
//                    producing(conf, bjob.toString(), innerRuleWorkInfo.resultStr);

                    //@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
                    SuccessCnt++;
                    System.out.println("Success count : "+SuccessCnt + ", Fail count : "+ FailCnt);

                    if (SuccessCnt >= 500) { //�ִ� 500�� get
                        consumer.commitSync(); //commit
                        SuccessCnt = 0;
                        FailCnt = 0;
                    }
                    //@@@@@@@@@@@@@@@@@@@@@@@RULE ���� ����@@@@@@@@@@@@@@@@@@@@@@@@@@@
                    producing(bjob.toString(), innerRuleWorkInfo.resultStr);

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

    public void producing(String producingData, String resultStr){

        String clanTopic = "CLAN";
        String ruleSTopic = "RULES_SAVE";
        String ruleFTopic = "RULEF_SAVE";
        int num = 0;


        ProducerRecord<String, String> clanRecord, ruleSRecord, ruleFRecord;
        clanRecord = new ProducerRecord<>(clanTopic, producingData);
        ruleSRecord = new ProducerRecord<>(ruleSTopic, producingData);
        ruleFRecord = new ProducerRecord<>(ruleFTopic, producingData);

        try {
            //1. Rule ���� , Clan ���� �̵�
            if(resultStr == "true") {
                System.out.println("CLAN MESSAGE PRODUCING:::::: " + producingData);
                //Ruleó���� �̵��ϴ� �޽���
                producer.send(clanRecord, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("CLAN TOPIC SENDING EXCEPTION :: " + exception.toString());
                    }
                });
                System.out.println("RULE SUCCESS SAVE PRODUCING:::::: " + producingData);
                //�����̷� �������� �̵��ϴ� �޽���
                producer.send(ruleSRecord, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("RULE SUCCESS TOPIC SENDING EXCEPTION ::" + exception.toString());
                    }
                });
            //2. Rule ����
            }else {
                System.out.println("RULE FAIL SAVE PRODUCING:::::: " + producingData);
                //�����̷� �������� �̵��ϴ� �޽���
                producer.send(ruleFRecord, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("RULE FAIL TOPIC SENDING EXCEPTION ::" + exception.toString());
                    }
                });
            }

        } catch (Exception e) {
            System.out.println("PRODUCING EXCEPTION ::" + e.toString());
        } finally {
            producer.flush();
        }
    }
    private void  __InnerRuleRun(InnerRuleWorkInfo info)
    {
//        info.resultStr = svrBridge.invokeRule(info.workinfo.hashmap, classpath, getAddCampId(info.real_flow_id));
        // �� ���� ��� ���� ���� ����
        //�ӽ�
        if(Integer.parseInt(info.workinfo.get("����").toString()) > 20 ){
            info.resultStr = "false";
        }

        info.countDownLatch.countDown();
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("RuleConsumer start ::::::::::::::::::::::::::::");
        // ���� ���� Thread ����
        // Lambda Runnable
        Runnable workTask =
                ()->{
                    InnerRuleWorkInfo info;

                    while(isClose != true)
                    {
                        try
                        {

                            info = InnerRuleWorkInfoQueue.poll(1000, TimeUnit.MILLISECONDS);
                            if(info == null)
                                continue;

                            //�� ���� �� �ð� üũ
                            info.starttime = System.currentTimeMillis();
                            __InnerRuleRun(info);
                            info.elapsedtime = System.currentTimeMillis();

                        }
                        catch (Exception e)
                        {
                            if(e instanceof InterruptedException)
                                isClose = true;
                        }
                    }
                };

        innerRuleWorkThreadCount = multiExecCount;
        innerRuleWorkThread = new Thread[innerRuleWorkThreadCount];
        for(int i=0; i<innerRuleWorkThreadCount; i++)
        {
            innerRuleWorkThread[i] = new Thread(workTask);
            innerRuleWorkThread[i].setName("RuleWork" + i);
            innerRuleWorkThread[i].start();
        }



        RuleConsumer ruleConsumer = new RuleConsumer("192.168.20.57:9092","test-consumer-group","RULE");
        polling(ruleConsumer.consumer);
    }
}
