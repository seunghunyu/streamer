package com.realtime.streamer.rule;

import com.realtime.streamer.cosumer.DataConsumer;
import com.realtime.streamer.data.RuleExF;
import com.realtime.streamer.data.RuleExS;
import com.realtime.streamer.repository.JdbcTemplateHistorySaveRepository;
import com.realtime.streamer.util.Utility;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.json.JsonParseException;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@EnableAsync
@RequiredArgsConstructor
//@Component
public class RuleSuccessSaveConsumer implements DataConsumer, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "RULE";
    Properties configs;
    KafkaConsumer<String, String> consumer;
    int lastUpdate = 0;
    String tableDt = "";
    String inst_Qry = " INSERT INTO R_REBM_RULE_REX_SLIST_1 (REBM_DETECT_ID, EX_CAMP_ID, STEP_ID, DETC_ROUTE_ID, WORK_DTM_MIL, STOP_NODE_ID, EX_TERM," +
                      "                                      CAMP_ID, CUST_ID, REAL_FLOW_ID, STOP_NODE_ITEM) " +
                      "                             VALUES (?,?,?,?,?,?,?,?,?,?,?)";

    @Autowired
    Utility utility;

    @Autowired
    JdbcTemplateHistorySaveRepository historySaveRepository;

    public RuleSuccessSaveConsumer(String address, String groupId, String topic) {
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
        System.out.println("RuleFailSave SAVE POLLING START@@@@@@@@@@@@@@@@@@@@");
        int SuccessCnt = 0;
        List<RuleExS> ruleExSList = new ArrayList<>();
        String detcChanSqlInfoItem;
        RuleExS ruleExS;

        try{

            while(true){

                if(lastUpdate + 600000 < LocalTime.now().getSecond()){
                    //사용중인 감지채널 조회, 감지테이블 저장 쿼리 조회, 감지 테이블 저장 아이템 조회
                    tableDt = utility.getTableDtNum();
                    inst_Qry = inst_Qry.replaceAll("R_REBM_RULE_REX_SLIST_0", "R_REBM_RULE_REX_SLIST_"+tableDt);
                }

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500)); //데이터가 없을 경우 최대 0.5초 기다림
                System.out.println("records :::" + records + ":::count ::"+Integer.toString(records.count()));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("!@#RULE FAIL SAVE  ::::" + record.value());

                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(record.value());
                    String detcChanCd = "";
                    //감지채널 key 값 존재시에만 데이터 세팅

                    ruleExS = new RuleExS();
                    ruleExS.setRebmDetcId(BigDecimal.valueOf(Double.parseDouble(bjob.get("REBM_DETECT_ID").toString())));
                    ruleExS.setExCampId(bjob.get("WORK_SVR_NM").toString());
                    ruleExS.setStepId(bjob.get("WORK_SVR_ID").toString());
                    ruleExS.setDetcRouteId(bjob.get("DETC_CHAN_CD").toString());
                    ruleExS.setWorkDtmMil("T");
                    ruleExS.setCampId("");
                    ruleExS.setCustId(bjob.get("CUST_ID").toString());
                    ruleExS.setRealFlowId("");

                    ruleExSList.add(ruleExS);

                    SuccessCnt++;
                    if (SuccessCnt >= 500) { //최대 500건 get

                        historySaveRepository.batchInsertRuleS(ruleExSList, inst_Qry);
                        System.out.println("RULE_SUCCESS_SAVE Success count : "+SuccessCnt);
                        consumer.commitSync(); //commit
                        SuccessCnt = 0;
                        ruleExSList.clear();

                        //break loop; //탈출
                    }
                    //consumer.commitSync(); //commit
                }

                if(ruleExSList.size() > 0) {
                    historySaveRepository.batchInsertRuleS(ruleExSList, inst_Qry);
                    //System.out.println("RULE_FAIL_SAVE Success count : "+SuccessCnt);
                    consumer.commitSync(); //commit
                    SuccessCnt = 0;
                    ruleExSList.clear();
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
        System.out.println("RuleConsumer start ::::::::::::::::::::::::::::");
        RuleSuccessSaveConsumer ruleSuccessSaveConsumer = new RuleSuccessSaveConsumer("192.168.20.57:9092","test-consumer-group","RULE_SUCCESS_SAVE");
        polling(ruleSuccessSaveConsumer.configs, ruleSuccessSaveConsumer.consumer);
    }
}
