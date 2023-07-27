package com.realtime.streamer.chan;

import com.realtime.streamer.cosumer.DataConsumer;
import com.realtime.streamer.data.ChanEx;
import com.realtime.streamer.data.RuleExS;
import com.realtime.streamer.repository.JdbcTemplateHistorySaveRepository;
import com.realtime.streamer.rule.RuleSuccessSaveConsumer;
import com.realtime.streamer.util.Utility;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.json.JsonParseException;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ChanSaveConsumer implements DataConsumer, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "RULE";
    Properties configs;
    KafkaConsumer<String, String> consumer;
    int lastUpdate = 0;
    String tableDt = "";
    String inst_Qry = " INSERT INTO R_REBM_CHAN_EX_LIST_1 (REBM_DETECT_ID, EX_CAMP_ID, ACT_ID, STAT_CD, WORK_SVR_NM, WORK_SVR_ID, CHAN_CD," +
            "                                      CUST_ID, SCRT_ID, REBM_SEND_ID, DETC_CHAN_CD, CONT_SET_YN, WORK_DTM_MIL, CAMP_ID, EX_ACT_ID, STEP_ID," +
            "                                      DETC_ROUTE_ID, ABT_OBJ_KIND, ABT_OBJ_ID, EX_TERM, WORK_DTM, REAL_FLOW_ID ) " +
            "                             VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,? )";

    @Autowired
    Utility utility;

    @Autowired
    JdbcTemplateHistorySaveRepository historySaveRepository;

    public ChanSaveConsumer(String address, String groupId, String topic) {
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
        this.tableDt = utility.getTableDtNum();
    }

    @Override
    public void polling(Properties conf, Consumer consumer) {
        System.out.println("ChanSave POLLING START@@@@@@@@@@@@@@@@@@@@");
        int SuccessCnt = 0;
        List<ChanEx> chanExList = new ArrayList<>();
        String detcChanSqlInfoItem;
        ChanEx chanEx;

        try{

            while(true){

                if(lastUpdate + 600000 < LocalTime.now().getSecond()){
                    //사용중인 감지채널 조회, 감지테이블 저장 쿼리 조회, 감지 테이블 저장 아이템 조회
                    tableDt = utility.getTableDtNum();
                    inst_Qry = inst_Qry.replaceAll("R_REBM_CHAN_EX_LIST_1", "R_REBM_CHAN_EX_LIST_"+tableDt);
                }

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500)); //데이터가 없을 경우 최대 0.5초 기다림
                System.out.println("records :::" + records + ":::count ::"+Integer.toString(records.count()));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("!@#CHAN SAVE  ::::" + record.value());

                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(record.value());
                    String detcChanCd = "";


                    chanEx = new ChanEx();
                    chanEx.setRebmDetcId(BigDecimal.valueOf(Double.parseDouble(bjob.get("REBM_DETECT_ID").toString())));
                    chanEx.setExCampId(bjob.get("WORK_SVR_NM").toString());
                    chanEx.setActId(bjob.get("WORK_SVR_ID").toString());
                    chanEx.setStatCd(bjob.get("DETC_CHAN_CD").toString());
                    chanEx.setWorkSvrNm("T");
                    chanEx.setWorkSvrId("");
                    chanEx.setChanCd(bjob.get("CUST_ID").toString());
                    chanEx.setCustId("");
                    chanEx.setScrtId("");
                    chanEx.setRebmSendId("");
                    chanEx.setDetcChanCd("");
                    chanEx.setContSetYn("");
                    chanEx.setWorkDtmMil("");
                    chanEx.setCampId("");
                    chanEx.setExActId("");
                    chanEx.setStepId("");
                    chanEx.setDetcRouteId("");
                    chanEx.setAbtObjKind("");
                    chanEx.setAbtObjId("");
                    chanEx.setExTerm("");
                    chanEx.setWorkDtm("");
                    chanEx.setRealFlowId("");

                    chanExList.add(chanEx);

                    SuccessCnt++;
                    if (SuccessCnt >= 500) { //최대 500건 get

                        historySaveRepository.batchInsertChan(chanExList, inst_Qry);
                        System.out.println("CHAN_SAVE Success count : "+SuccessCnt);
                        consumer.commitSync(); //commit
                        SuccessCnt = 0;
                        chanExList.clear();

                        //break loop; //탈출
                    }
                    //consumer.commitSync(); //commit
                }

                if(chanExList.size() > 0) {
                    historySaveRepository.batchInsertChan(chanExList, inst_Qry);
                    //System.out.println("RULE_FAIL_SAVE Success count : "+SuccessCnt);
                    consumer.commitSync(); //commit
                    SuccessCnt = 0;
                    chanExList.clear();
                }
            }
        }catch(JsonParseException e){
            System.out.println(e.getMessage());
            //e.printStackTrace();
        }catch(Exception e) {
            System.out.println("GATHER SAVE CONSUMING ERROR ::: "+e.getMessage());

        }finally{

        }
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("ChanSaveConsumer start ::::::::::::::::::::::::::::");
        ChanSaveConsumer chanSaveConsumer = new ChanSaveConsumer("192.168.20.57:9092","test-consumer-group","RULE_SUCCESS_SAVE");
        polling(chanSaveConsumer.configs, chanSaveConsumer.consumer);
    }
}
