package com.realtime.streamer.assign;

import com.realtime.streamer.Queue.AssignQueue;
import com.realtime.streamer.consumer.CoWorker;
import com.realtime.streamer.consumer.Worker;
import com.realtime.streamer.data.Camp;
import com.realtime.streamer.service.CampService;
import com.realtime.streamer.util.Utility;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalTime;
import java.util.*;

@Order(4)
@EnableAsync
//@RequiredArgsConstructor
@Component
public class AssignWorker implements Worker, CommandLineRunner {

    int lastUpdate = 0;

    String table_dt = "", toDate = "", toTime = "", toMonth = "";
    int seqNo = 0;
    int campCnt = 0;

    java.text.DateFormat dateFormat1 = new java.text.SimpleDateFormat("yyyyMMdd");
    java.text.DateFormat dateFormat2 = new java.text.SimpleDateFormat("HHmm");
    java.text.DateFormat dfhhmmss = new java.text.SimpleDateFormat("HHmmss");

    HashMap<String,String> hashChanFlowId = new HashMap<String,String>();
    AssignQueue assignQueue = new AssignQueue();

    //--- 공통수행SQL구문
    ArrayList<String> commDB_POOL = new ArrayList<String>();
    ArrayList<String> commSEL_COLM = new ArrayList<String>();
    ArrayList<String> commSEL_TYPE = new ArrayList<String>();
    ArrayList<String> commSQL_SCRT = new ArrayList<String>();
    ArrayList<String> commVAR_NM = new ArrayList<String>();
    ArrayList<String> commVAR_TY = new ArrayList<String>();
    ArrayList<String> commSQL_ID = new ArrayList<String>();
    HashMap<String,String> detcChanSqlId = new HashMap<String,String>();

    HashMap<String,String> hashFlowChan_RouteIds = new HashMap<String,String>();
    HashMap<String,String> hashFlowChan_CampIds = new HashMap<String,String>();
    HashMap<String,String> hashFlowChan_FlowIds = new HashMap<String,String>();
    HashMap<String,String> hashCampFsave = new HashMap<String,String>();         // 캠페인 실패시 Rule 테이블 저장 여부

    HashMap<String,String> hashTgetStup = new HashMap<String,String>();   // 특정 대상 설정 정보
    HashMap<String,String> hashRemStup  = new HashMap<String,String>();
    HashMap<String,String> hashFlowId_WaveId = new HashMap<String,String>();   // Wave 연계
    HashMap<String,String> hashFlowId_WaveActIds = new HashMap<String,String>();
    HashMap<String,String> hashFlowId_WaveFltYn = new HashMap<String,String>();


    boolean IMDGSave = false;
    String REBM_WAVE_FLT_ITEM = "";

    HashMap<String, String> hashFlowId_ExCampId = new HashMap<String, String>();  // EX_CAMP_ID 정보 저장
    HashMap<String, String> hashFlowId_CampId = new HashMap<String, String>();

    long autoAlarmTime = 0, autoAlarmCount = 0;
    boolean tgetfilteryn = false;


    @Autowired
    CampService campService;

    @Autowired
    Utility utility;

    public AssignWorker() {
        System.out.println("call Assign Worker Constructor");
        this.lastUpdate = LocalTime.now().getSecond();
    }


    @Override
    public void polling(){
        int SuccessCnt = 0;
        int FailCnt = 0;

        try{
            loop:
            while(true){

                if(lastUpdate + 8 < LocalTime.now().getSecond()){
                   // System.out.println("AssingWorkerQueue Size :::::::::::::" + this.assignQueue.getAssignWorkQ().toString()  + " not this :::" + assignQueue.getAssignWorkQ().toString());
                    //룰 성공 실패이력 저장 여부 조회
                    setCampRuleSave();
                    //공통 수행 SQL 구문 관리 조회
                    getCommonExecSql();

                    setCampRouteId();

                    //당일 수행 캠페인 ID 추출
                    setExDtCampList();

                    //수행오래걸릴경우 알람 찍어주는 로직
                    setAutoAlarmInfo();

                }

                if(assignQueue.getAssignWorkQ().size() == 0){
                    continue;
                }else if(assignQueue.getAssignWorkQ().size() > 0){
                    String assignWorkItem = assignQueue.getWorkQueueItem();
                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(assignWorkItem);
                    System.out.println("AssingWorker !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + bjob.toString());




                    //Assign Producing 큐로 데이터 전달
                    assignQueue.addProdQueueItem(bjob.toString());

                }
            }
        }catch(JsonParseException e){
            System.out.println("JsonParsing Error:::" + e.getMessage());
            //e.printStackTrace();
        }catch(Exception e) {
            System.out.println("Assign Worker Exception:::" + e.getMessage());
        }finally{

        }

    }
    //수행오래걸릴경우 알람 찍어주는 로직
    private void setAutoAlarmInfo() {
    }
    //캠페인별 감지채널별 ROUTE_ID 추출
    private void setCampRouteId() {
    }
    //공통 수행 SQL 구문 관리 조회
    private void getCommonExecSql() {



    }
    //룰 성공 실패이력 저장 여부 조회
    private void setCampRuleSave() {
        hashCampFsave.clear();
//        SELECT CAMP_ID, RULE_S_SAVE_YN, RULE_F_SAVE_YN  FROM R_PLAN
        List<Camp> ruleHistSaveYnList = campService.getRuleHistSaveYn();
        for(var i = 0 ; i < ruleHistSaveYnList.size() ; i++){
            hashCampFsave.put(ruleHistSaveYnList.get(i).getCampId(), ruleHistSaveYnList.get(i).getRuleFSaveYn());
        }

    }


    @Override
    public void work() {

    }

    /**
     * 당일 flow 별 수행 CAMP_ID 조합
     */
//    public void setCampOlapp103List(){
    public void setExDtCampList(){
        hashFlowId_ExCampId.clear();
        java.text.DateFormat dateFormat = new java.text.SimpleDateFormat("yyyyMMdd");
        List<Camp> exCampList = campService.getExCampStatList(dateFormat.format(new Date()));
        for(int i = 0 ; i < exCampList.size() ; i++){
            hashFlowId_ExCampId.put(exCampList.get(i).getRealFlowId(), exCampList.get(i).getExCampId());
        }
    }


    @Async
    @Override
    public void run(String... args) throws Exception {
        System.out.println("Assign  Worker START::::::::::::::::::::::::::::::::::");
        AssignWorker assignWorker = new AssignWorker();

//        polling(assignWorker.assignQueue);
        polling();

    }


}
