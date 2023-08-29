package com.realtime.streamer.assign;

import com.realtime.streamer.Queue.AssignQueue;
import com.realtime.streamer.consumer.CoWorker;
import com.realtime.streamer.data.Camp;
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

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
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
public class AssignConsumer implements CoWorker, CommandLineRunner {
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

    public AssignConsumer(String address, String groupId, String topic) {
        System.out.println("call Assign Consumer Constructor");
        this.Address = address;
        this.GroupId = groupId;
        this.topic = topic;
        this.lastUpdate = LocalTime.now().getSecond();
    }


    @Override
    public void polling(Consumer consumer, Producer producer) throws Exception {
        int SuccessCnt = 0;
        int FailCnt = 0;
        if(lastUpdate + 7 < LocalTime.now().getSecond()){
            setChanCampId();
        }

        try{
            loop:
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500)); //데이터가 없을 경우 최대 0.5초 기다림

                if(records.count() == 0) continue ;

                System.out.println("Assign Consumer records count ::"+Integer.toString(records.count()));

                for (ConsumerRecord<String, String> record : records) {
                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(record.value());


                    // 캠페인 목록이 없는 경우 Polling 후 Rule 전달하지 않음.
                    if(hashChanFlowId.isEmpty() || hashChanFlowId.size() == 0) {
                        System.out.println(" Campaign mapping not found return = " + bjob.toString());
                        break;
                    }else{
                        System.out.println("RUNNING REALTIME CAMPAIGN ::::::::::::::::::" + hashChanFlowId);
                    }

                    if(bjob.get("DETC_CHAN_CD").toString() == null || hashChanFlowId.get(bjob.get("DETC_CHAN_CD").toString()) == null)  // 감지채널에 캠페인이 매핑되지 않는 경우
                    {
                        System.out.println(("Campaign mapping is null : REBM_DETECT_ID=" + bjob.get("REBM_DETECT_ID").toString() + ", DETC_CHAN_CD=" + bjob.get("DETC_CHAN_CD").toString()));
                        break;
                    }else{
                        bjob.put("FLOW_IDS", hashChanFlowId.get(bjob.get("DETC_CHAN_CD")));

                    }


                    //Assign WorkQueue로 데이터
                    System.out.println("Assing Consumer !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + bjob.toString());
                    assignQueue.addWorkQueueItem(bjob.toString());

                    System.out.println("Success count : "+SuccessCnt);
                    if (SuccessCnt >= 1000) { //최대 500건 get
                        consumer.commitSync(); //commit
                        SuccessCnt = 0;
                    }
                    consumer.commitSync(); //commit
                    //producing(producer, bjob.toString());
                    SuccessCnt++;
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

//SELECT T2.DETC_CHAN_CD, T2.REAL_FLOW_ID
//   FROM R_REBM_FLOW_INFO T1, R_FLOW_DETC_CHAN T2
//   WHERE T1.REAL_FLOW_ID = T2.REAL_FLOW_ID
//     AND T1.STAT_CD IN ('3000', '3100')
//     AND T1.STR_DT <= ?
//     AND T1.END_DT >= ?
//   ORDER BY T2.DETC_CHAN_CD, T1.CAMP_PRITY, T2.REAL_FLOW_ID
    private void setChanCampId() throws Exception
    {
        if (table_dt == null || table_dt == "") {
           table_dt = utility.getTableDtNum();
        }

        String detc_chan = "", old_detc_chan = "", flow_ids = "";
        List<Camp> exFlowInfo = campService.getExCampChanInfo(dateFormat1.format(new java.util.Date()).toString());

        hashChanFlowId.clear();
        for (int i = 0; i < exFlowInfo.size(); i++) {
            detc_chan = exFlowInfo.get(i).getDetcChanCd();
            if ((old_detc_chan != "") && (!old_detc_chan.equals(detc_chan))) {
                hashChanFlowId.put(old_detc_chan, flow_ids.substring(1));
                flow_ids = "";
            }
            old_detc_chan = detc_chan;
            flow_ids += "," + exFlowInfo.get(i).getRealFlowId();
        }
        if (flow_ids.length() > 1) {
            hashChanFlowId.put(old_detc_chan, flow_ids.substring(1));
        }
    }

    public void producing(Producer producer, String producingData){

        String ruleTopic = "RULE";
        String ruleFTopic = "RULEF_SAVE";

        int num = 0;

        ProducerRecord<String, String> record, record2;
        record = new ProducerRecord<>(ruleTopic, producingData);
        record2 = new ProducerRecord<>(ruleFTopic, producingData);

        try {
            //  Thread.sleep(2000);
            System.out.println("RULE MESSAGE PRODUCING:::::: " + producingData);
            //Rule처리로 이동하는 메시지
            if(producer == null){
                System.out.println("@@@@@@@@@@@@@@@@@@gather consumer producer is null");
            }
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("RULE TOPIC SENDING EXCEPTION :: "+ exception.toString());
                }
            });
            System.out.println("RULE FAIL SAVE PRODUCING:::::: " + producingData);
            //감지이력 저장으로 이동하는 메시지
            producer.send(record2, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("RULE FAIL SAVE TOPIC SENDING EXCEPTION ::" + exception.toString());
                }
            });
        } catch (Exception e) {
            System.out.println("Assgin Consumer PRODUCING EXCEPTION ::" + e.toString());
        } finally {
            producer.flush();
        }

    }

    @Async
    @Override
    public void run(String... args) throws Exception {
        System.out.println("Gather Consumer START::::::::::::::::::::::::::::::::::");
        AssignConsumer gatherConsumer = new AssignConsumer("192.168.20.57:9092","test-consumer-group","ASSIGN");
        gatherConsumer.consumerConfigs = utility.setKafkaConsumerConfigs(gatherConsumer.Address, gatherConsumer.GroupId);
        gatherConsumer.producerConfigs = utility.setKafkaProducerConfigs(gatherConsumer.Address);

        gatherConsumer.consumer = new KafkaConsumer<String, String>(gatherConsumer.consumerConfigs);
        gatherConsumer.producer = new KafkaProducer<String, String>(gatherConsumer.producerConfigs);
        gatherConsumer.consumer.subscribe(Arrays.asList(gatherConsumer.topic));

        polling(gatherConsumer.consumer, gatherConsumer.producer);
    }
}
