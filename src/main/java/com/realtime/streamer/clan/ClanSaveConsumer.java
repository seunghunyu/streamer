package com.realtime.streamer.clan;

import com.realtime.streamer.consumer.DataConsumer;
import com.realtime.streamer.data.ClanEx;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@EnableAsync
@RequiredArgsConstructor
//@Component
public class ClanSaveConsumer implements DataConsumer, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "RULE";
    Properties configs;
    KafkaConsumer<String, String> consumer;
    int lastUpdate = 0;
    String tableDt = "";
    String inst_Qry = " INSERT INTO R_REBM_CLAN_EX_LIST_1 (REBM_DETECT_ID, EX_CAMP_ID, ACT_ID, STAT_CD, WORK_SVR_NM, CHAN_CD, CUST_ID," +
            "                                            OLPP_EXD_BRCH_CD, OLPP_EXD_KIND_CD, WORK_DTM_MIL, CLAN_DESC, " +
            "                                                     STEP_ID, DETC_ROUTE_ID, CAMP_ID, EX_TERM, WORK_DTM, REAL_FLOW_ID) " +
            "                             VALUES (?,?,?,?,?,?,?,?,?,?,? ?,?,?,?,?,?)";

    @Autowired
    Utility utility;

    @Autowired
    JdbcTemplateHistorySaveRepository historySaveRepository;

    public ClanSaveConsumer(String address, String groupId, String topic) {
        this.Address = address;
        this.GroupId = groupId;
        this.topic = topic;
        this.lastUpdate = LocalTime.now().getSecond();
    }

    @Override
    public void polling(Consumer consumer) {
        System.out.println("CLANSave SAVE POLLING START@@@@@@@@@@@@@@@@@@@@");
        int SuccessCnt = 0;
        List<ClanEx> clanExList = new ArrayList<>();
        ClanEx clanEx;

        try{

            while(true){

                if(lastUpdate + 600000 < LocalTime.now().getSecond()){
                    //사용중인 감지채널 조회, 감지테이블 저장 쿼리 조회, 감지 테이블 저장 아이템 조회
                    tableDt = utility.getTableDtNum();
                    inst_Qry = inst_Qry.replaceAll("R_REBM_CLAN_EX_LIST_1", "R_REBM_CLAN_EX_LIST_"+tableDt);
                }

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500)); //데이터가 없을 경우 최대 0.5초 기다림
                System.out.println("records :::" + records + ":::count ::"+Integer.toString(records.count()));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("!@#CLAN SAVE  ::::" + record.value());

                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(record.value());
                    String detcChanCd = "";
                    //감지채널 key 값 존재시에만 데이터 세팅

                    clanEx = new ClanEx();

                    clanEx.setRebmDetcId(BigDecimal.valueOf(Double.parseDouble(bjob.get("REBM_DETECT_ID").toString())));
                    clanEx.setExCampId(bjob.get("WORK_SVR_NM").toString());
                    clanEx.setActId(bjob.get("WORK_SVR_ID").toString());
                    clanEx.setStatCd(bjob.get("DETC_CHAN_CD").toString());
                    clanEx.setWorkSvrNm("");
                    clanEx.setChanCd("");
                    clanEx.setCustId(bjob.get("CUST_ID").toString());
                    clanEx.setOlppExdBrchCd("");
                    clanEx.setOlppExdKindCd("");
                    clanEx.setWorkDtmMil("");
                    clanEx.setClanDesc("");
                    clanEx.setStepId("");
                    clanEx.setDetcRouteId("");
                    clanEx.setCampId("");
                    clanEx.setExTerm("");
                    clanEx.setWorkDtm("");
                    clanEx.setRealFlowId("");

                    clanExList.add(clanEx);

                    SuccessCnt++;
                    if (SuccessCnt >= 500) { //최대 500건 get

                        historySaveRepository.batchInsertClan(clanExList, inst_Qry);
                        System.out.println("CLAN_SAVE Success count : "+SuccessCnt);
                        consumer.commitSync(); //commit
                        SuccessCnt = 0;
                        clanExList.clear();

                        //break loop; //탈출
                    }
                    //consumer.commitSync(); //commit
                }

                if(clanExList.size() > 0) {
                    historySaveRepository.batchInsertClan(clanExList, inst_Qry);
                    consumer.commitSync(); //commit
                    SuccessCnt = 0;
                    clanExList.clear();
                }
            }
        }catch(JsonParseException e){
            System.out.println(e.getMessage());
            //e.printStackTrace();
        }catch(Exception e) {
            System.out.println("CLAN SAVE CONSUMING ERROR ::: "+e.getMessage());

        }finally{

        }
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("CLANSaveConsumer start ::::::::::::::::::::::::::::");
        ClanSaveConsumer clanSaveConsumer = new ClanSaveConsumer("192.168.20.57:9092","test-consumer-group","CLAN_SAVE");
        clanSaveConsumer.configs = utility.setKafkaConsumerConfigs(clanSaveConsumer.Address, clanSaveConsumer.GroupId);
        clanSaveConsumer.consumer = new KafkaConsumer<String, String>(clanSaveConsumer.configs);
        clanSaveConsumer.consumer.subscribe(Arrays.asList(clanSaveConsumer.topic)); // 구독할 topic 설정
        polling(clanSaveConsumer.consumer);
    }
}
