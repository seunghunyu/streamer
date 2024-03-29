package com.realtime.streamer.rule;

import com.realtime.streamer.rebminterface.DataConsumer;
import com.realtime.streamer.data.RuleExF;
import com.realtime.streamer.repository.rebm.JdbcTemplateHistorySaveRepository;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;

@EnableAsync
@RequiredArgsConstructor
//@Component
public class RuleFailSaveConsumer implements DataConsumer, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "RULE";
    Properties configs;
    KafkaConsumer<String, String> consumer;
    int lastUpdate = 0;
    String inst_Qry = " INSERT INTO R_REBM_RULE_REX_FLIST_1 (REBM_DETECT_ID, EX_CAMP_ID, STEP_ID, DETC_ROUTE_ID, WORK_DTM_MIL, STOP_NODE_ID, EX_TERM," +
                      "                                      CAMP_ID, CUST_ID, REAL_FLOW_ID, STOP_NODE_ITEM) " +
                      "                             VALUES (?,?,?,?,?,?,?,?,?,?,?)";
    String tableDt = "";
    @Autowired
    Utility utility;

    @Autowired
    JdbcTemplateHistorySaveRepository historySaveRepository;

    public RuleFailSaveConsumer(String address, String groupId, String topic) {
        this.Address = address;
        this.GroupId = groupId;
        this.topic = topic;
        this.lastUpdate = LocalTime.now().getSecond();

        this.configs = utility.setKafkaConsumerConfigs(this.Address, this.GroupId);

        this.consumer = new KafkaConsumer<String, String>(this.configs);
        this.consumer.subscribe(Arrays.asList(topic)); // 구독할 topic 설정
        this.tableDt = utility.getTableDtNum();

    }

    @Override
    public void polling(Consumer consumer) {
        System.out.println("RuleFailSave SAVE POLLING START@@@@@@@@@@@@@@@@@@@@");
        int SuccessCnt = 0;
        List<RuleExF> ruleExFList = new ArrayList<>();
        String detcChanSqlInfoItem;
        RuleExF ruleExF;

        try{

            while(true){

                if(lastUpdate + 6 < LocalTime.now().getSecond()){
                    //사용중인 감지채널 조회, 감지테이블 저장 쿼리 조회, 감지 테이블 저장 아이템 조회
                    tableDt = utility.getTableDtNum();
                    inst_Qry = inst_Qry.replaceAll("R_REBM_RULE_REX_FLIST_1", "R_REBM_RULE_REX_FLIST_"+tableDt);

                }
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500)); //데이터가 없을 경우 최대 0.5초 기다림
                System.out.println("records :::" + records + ":::count ::"+Integer.toString(records.count()));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("!@#RULE FAIL SAVE  ::::" + record.value());

                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(record.value());
                    String detcChanCd = "";


                    ruleExF = new RuleExF();
                    ruleExF.setRebmDetcId(BigDecimal.valueOf(Double.parseDouble(bjob.get("REBM_DETECT_ID").toString())));
                    ruleExF.setExCampId(bjob.get("WORK_SVR_NM").toString());
                    ruleExF.setStepId(bjob.get("WORK_SVR_ID").toString());
                    ruleExF.setDetcRouteId(bjob.get("DETC_CHAN_CD").toString());
                    ruleExF.setWorkDtmMil("T");
                    ruleExF.setStopNodeId("");
                    ruleExF.setExTerm("T");
                    ruleExF.setCampId("");
                    ruleExF.setCustId(bjob.get("CUST_ID").toString());
                    ruleExF.setRealFlowId("");
                    ruleExF.setStopNodeItem("");

                    ruleExFList.add(ruleExF);

                    SuccessCnt++;
                    if (SuccessCnt >= 500) { //최대 500건 get

                        historySaveRepository.batchInsertRuleF(ruleExFList, inst_Qry);
                        System.out.println("RULE_FAIL_SAVE Success count : "+SuccessCnt);
                        consumer.commitSync(); //commit
                        SuccessCnt = 0;
                        ruleExFList.clear();

                        //break loop; //탈출
                    }
                    //consumer.commitSync(); //commit
                }

                if(ruleExFList.size() > 0) {
                    historySaveRepository.batchInsertRuleF(ruleExFList, inst_Qry);
                    //System.out.println("RULE_FAIL_SAVE Success count : "+SuccessCnt);
                    consumer.commitSync(); //commit
                    SuccessCnt = 0;
                    ruleExFList.clear();
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
        RuleFailSaveConsumer ruleFailSaveConsumer = new RuleFailSaveConsumer("192.168.20.57:9092","test-consumer-group","RULE_FAIL_SAVE");
        polling(ruleFailSaveConsumer.consumer);
    }
}
