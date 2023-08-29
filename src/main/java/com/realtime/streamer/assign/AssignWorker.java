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
import org.springframework.jdbc.core.JdbcTemplate;
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

    //--- �������SQL����
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
    HashMap<String,String> hashCampFsave = new HashMap<String,String>();         // ķ���� ���н� Rule ���̺� ���� ����

    HashMap<String,String> hashTgetStup = new HashMap<String,String>();   // Ư�� ��� ���� ����
    HashMap<String,String> hashRemStup  = new HashMap<String,String>();
    HashMap<String,String> hashFlowId_WaveId = new HashMap<String,String>();   // Wave ����
    HashMap<String,String> hashFlowId_WaveActIds = new HashMap<String,String>();
    HashMap<String,String> hashFlowId_WaveFltYn = new HashMap<String,String>();


    boolean IMDGSave = false;
    String REBM_WAVE_FLT_ITEM = "";

    HashMap<String, String> hashFlowId_ExCampId = new HashMap<String, String>();  // EX_CAMP_ID ���� ����
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

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Override
    public void polling(){
        int SuccessCnt = 0;
        int FailCnt = 0;

        try{
            loop:
            while(true){

                if(lastUpdate + 8 < LocalTime.now().getSecond()){
                   // System.out.println("AssingWorkerQueue Size :::::::::::::" + this.assignQueue.getAssignWorkQ().toString()  + " not this :::" + assignQueue.getAssignWorkQ().toString());
                    //�� ���� �����̷� ���� ���� ��ȸ
                    setCampRuleSave();
                    //���� ���� SQL ���� ���� ��ȸ
                    getCommonExecSql();

                    setCampRouteId();

                    //���� ���� ķ���� ID ����
                    setExDtCampList();

                    //��������ɸ���� �˶� ����ִ� ����
                    setAutoAlarmInfo();

                }

                if(assignQueue.getAssignWorkQ().size() == 0){
                    continue;
                }else if(assignQueue.getAssignWorkQ().size() > 0){
                    String assignWorkItem = assignQueue.getWorkQueueItem();
                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(assignWorkItem);
                    System.out.println("AssingWorker !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + bjob.toString());




                    //Assign Producing ť�� ������ ����
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
    //��������ɸ���� �˶� ����ִ� ����
    private void setAutoAlarmInfo() {
    }
    //ķ���κ� ����ä�κ� ROUTE_ID ����
    private void setCampRouteId() {
    }
    //���� ���� SQL ���� ���� ��ȸ
    private void getCommonExecSql() {
        //utility1.getCommonExecAllSql(svrBridge, commDB_POOL, commSEL_COLM, commSEL_TYPE, commSQL_SCRT, commVAR_NM, commVAR_TY, commSQL_ID, detcChanSqlId);

        commDB_POOL.clear();
        commSEL_COLM.clear();
        commSEL_TYPE.clear();
        commSQL_SCRT.clear();
        commVAR_NM.clear();
        commVAR_TY.clear();

        String qry = " SELECT SQL_ID, DB_POOL, SEL_COLM_GRP, SQL_SCRT1, SQL_SCRT2, SQL_SCRT3, RPLC_VAR_NM_GRP, RPLC_VAR_TY_GRP, SEL_TYPE_GRP "
                   + " FROM R_CMN_EX_SQL ORDER BY DB_POOL ";
        String sqlScrt = "";

        List<Map<String, Object>> maps = jdbcTemplate.queryForList(qry);
        for(int i = 0 ; i < maps.size() ; i++){
            sqlScrt = maps.get(i).get("SQL_SCRT1").toString() + ( maps.get(i).get("SQL_SCRT2") == null  ? "" : maps.get(i).get("SQL_SCRT2").toString())
                    + (maps.get(i).get("SQL_SCRT3") == null ? "" : maps.get(i).get("SQL_SCRT3").toString());

            sqlScrt = sqlScrt.replace("\n", " ").replace("\t", " ");
            commSQL_SCRT.add(sqlScrt);
            commSQL_ID.add(maps.get(i).get("SQL_ID").toString());
            commDB_POOL.add(maps.get(i).get("DB_POOL").toString());
            commSEL_COLM.add(maps.get(i).get("SEL_COLM_GRP").toString());
            commSEL_TYPE.add(maps.get(i).get("SEL_TYPE_GRP").toString());
            if(maps.get(i).get("RPLC_VAR_NM_GRP") == null || maps.get(i).get("RPLC_VAR_NM_GRP").toString().equals("")) { // ġȯ�±װ� ���� ���
                commVAR_NM.add("");
                commVAR_TY.add("");
            } else {
                commVAR_NM.add(maps.get(i).get("RPLC_VAR_NM_GRP").toString());
                commVAR_TY.add(maps.get(i).get("RPLC_VAR_TY_GRP").toString());
            }
        }

        String beforeDetcChanCd = "", sqlids = "";
        qry = " SELECT DETC_CHAN_CD, SQL_ID FROM R_DETC_CHAN_USE_CMN_SQL ORDER BY DETC_CHAN_CD ASC ";
        maps.clear();
        maps = jdbcTemplate.queryForList(qry);
        for(int i = 0 ; i < maps.size() ; i++) {
            if( (!maps.get(i).get("DETC_CHAN_CD").toString().equals(beforeDetcChanCd)) && beforeDetcChanCd.length() > 0) {
                detcChanSqlId.put(beforeDetcChanCd, sqlids.substring(1));
                sqlids = "";
            }
            beforeDetcChanCd = maps.get(i).get("DETC_CHAN_CD").toString();
            sqlids += ";" + maps.get(i).get("SQL_ID").toString();
        }

        if(sqlids.length() > 0) {
            detcChanSqlId.put(beforeDetcChanCd, sqlids.substring(1));
            sqlids = "";
        }

    }
    //�� ���� �����̷� ���� ���� ��ȸ
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
     * ���� flow �� ���� CAMP_ID ����
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
