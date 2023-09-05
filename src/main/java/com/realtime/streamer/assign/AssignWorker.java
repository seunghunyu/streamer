package com.realtime.streamer.assign;

import com.realtime.streamer.Queue.AssignQueue;
import com.realtime.streamer.rebminterface.Worker;
import com.realtime.streamer.data.Camp;
import com.realtime.streamer.service.CampService;
import com.realtime.streamer.util.Utility;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.json.JsonParseException;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

import java.time.LocalTime;
import java.util.*;

@Order(4)
@EnableAsync
//@RequiredArgsConstructor
@Component
public class AssignWorker implements Worker, CommandLineRunner {

    int lastUpdate = 0;

    String table_dt = "", toDate = "", toTime = "", toMonth = "";
    String strTime = "", endTime="";
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

    @Autowired
    @Qualifier("h2JdbcTemplate")
    JdbcTemplate H2JdbcTemplate;

    @Override
    public void polling(){
        int SuccessCnt = 0;
        int FailCnt = 0;

        try{
            loop:
            while(true){

                if(assignQueue.getAssignWorkQ().size() == 0){
                    continue;
                }else if(assignQueue.getAssignWorkQ().size() > 0){
                    if(lastUpdate + 8 < LocalTime.now().getSecond()){
                        // System.out.println("AssingWorkerQueue Size :::::::::::::" + this.assignQueue.getAssignWorkQ().toString()  + " not this :::" + assignQueue.getAssignWorkQ().toString());
                        //�� ���� �����̷� ���� ���� ��ȸ
                        setCampRuleSave();
                        //���� ���� SQL ���� ���� ��ȸ
                        getCommonExecSql();

                        setCampRouteId();

                        //���� ���� ķ���� ID ����
                        setExDtCampList();

                        //��������ɸ���� �˶� ����ִ� ������ �ð��� ī���� ���� ����
                        setAutoAlarmInfo();

                    }
                    ArrayList<String> arrFlowId1 = new ArrayList<String>();
                    ArrayList<String> arrFlowId2 = new ArrayList<String>();
                    ArrayList<String> arrFlowId21 = new ArrayList<String>();

                    String assignWorkItem = assignQueue.getWorkQueueItem();
                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(assignWorkItem);
                    System.out.println("AssingWorker !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + bjob.toString());

                    //1.�����ؾ� �� ķ���� ��� ����
                    if(bjob.get("FLOW_IDS") == null || "".equals(bjob.get("FLOW_IDS").toString())) {
                        System.out.println(bjob.get("REBM_DETECT_ID").toString() + " : FLOW_IDS is null.");
                    }
                    String flowIdList = bjob.get("FLOW_IDS") == null ? "" : bjob.get("FLOW_IDS").toString();
                    String[] flow_ids = flowIdList.split(",");  // �����Ϳ� , ���� ����

                    if(table_dt == null || table_dt.equals("0")) {
                        System.out.println(bjob.get("REBM_DETECT_ID").toString() +  " : TABLE_DT check=" + table_dt);
                    }

                    //2.ķ���� ���డ�� ���� �� �ð� ���� üũ
                    if(flow_ids.length > 0){
                        for(int i = 0 ; i < flow_ids.length ; i++){
                            if(flow_ids[i].length() > 1){
                                if(hashFlowId_ExCampId.get(flow_ids[i]) != null) {  // ���� ����ķ������ �ִ� ��츸 ó����.
                                    List<Camp> timeInfo = campService.getExCampTMInfo(flow_ids[i], bjob.get("OBZ_TODATE").toString());
                                    if(timeInfo.size() > 0){
                                        toTime = bjob.get("OBZ_TODATE").toString();
                                        strTime = timeInfo.get(0).getStrTm();
                                        endTime = timeInfo.get(0).getEndTm();
                                        // Ư�� �ð� �ȿ� �ִ� ���
                                        if(endTime.equals("0000") || (toTime.compareTo(strTime) >= 0 && toTime.compareTo(endTime) <= 0))
                                        {
                                            arrFlowId1.add(flow_ids[i]);
                                            //svrBridge.debug_println("temp" + tempCampId);
                                        }
                                    }

                                }
                            }
                        }
                    }
                    if(arrFlowId1 == null || arrFlowId1.size() == 0) continue;

                    //3.Ư�� ��� ����, ���� ���� ����
                    boolean isIMDG = false;
                    if(tgetfilteryn == false){
                        for(String flowid : arrFlowId1){
                            arrFlowId2.add(flowid);
                        }
                    }else{
//                        execIMDGTargetFilter(svrBridge, arrFlowId1, arrFlowId2, imdgClient);
                    }
                    if(arrFlowId2 == null || arrFlowId2.size() == 0) continue ;

                    //�����߼۰���
//                    execIMDGWaveFilter(svrBridge, arrFlowId2, arrFlowId21, imdgClient);
                    if(arrFlowId21 == null || arrFlowId21.size() == 0) continue ;

                    //4.�������͸�
                    if(isIMDG == false){
                        //------ 2 Ư�� ��� ���� ������ �ִ� ���
                        if(tgetfilteryn == false) {
                            for(String flowid : arrFlowId1)
                            {
                                arrFlowId2.add(flowid);
                            }
                        } else {
//                            execDBTargetFilter(svrBridge, arrFlowId1, arrFlowId2);
                        }
                    }
                    if(arrFlowId2 == null || arrFlowId2.size() == 0) continue ;
                    //-------------- 2.3 �����߼� ������ ���
//                    execDBWaveFilter(svrBridge, arrFlowId2, arrFlowId21);
                    if(arrFlowId21 == null || arrFlowId21.size() == 0) continue ;

                    //5.���� ����SQL ������ ��ȸ�Ͽ� ������ ����
                    getCommonExecSql();

                    if(arrFlowId21 != null && arrFlowId21.size()> 0){
                        String detc_chan_cd = bjob.get("DETC_CHAN_CD") == null ? "" : bjob.get("DETC_CHAN_CD").toString();
                        StringBuffer scampids = new StringBuffer();
                        StringBuffer sflowids = new StringBuffer();
                        StringBuffer srouteids = new StringBuffer();

                        for(String flowid : arrFlowId21){
                            if(hashFlowChan_RouteIds.get(flowid + detc_chan_cd) == null || hashFlowChan_RouteIds.get(flowid + detc_chan_cd).isEmpty()) {
                                setCampRouteId();  // ��� ���ο� ���� Route ID ������ ���� ��� ����
                            }

                            if(hashFlowChan_RouteIds.get(flowid + detc_chan_cd) == null) {  // �� Flow�� ���δٸ� ������ Detect_chan�� ���� �Ҽ� �����Ƿ�
                                System.out.println("flowid : " + flowid + ", detc_chan_cd = " + detc_chan_cd + " : RouteId is null");
                            }

                            scampids.append("," + hashFlowChan_CampIds.get(flowid + detc_chan_cd));
                            sflowids.append("," + hashFlowChan_FlowIds.get(flowid + detc_chan_cd));
                            srouteids.append("," + hashFlowChan_RouteIds.get(flowid + detc_chan_cd));
                        }
                        if(srouteids.toString().length() > 0) {
                            bjob.put("CAMP_IDS", scampids.toString().substring(1));
                            bjob.put("REAL_FLOW_IDS", sflowids.toString().substring(1));
                            bjob.put("DETC_ROUTE_IDS", srouteids.toString().substring(1));
                            //svrBridge.debug_println("111111=" + tmpWorkInfo.get("REAL_FLOW_IDS") + "###" + tmpWorkInfo.get("DETC_ROUTE_IDS") + "###" + tmpWorkInfo.get("CAMP_IDS"));

//                            if(tracelogyn && srouteids.toString().indexOf(tracelogid) >= 0)
//                                svrBridge.filelog_println("ruledata", tmpWorkInfo.get("REBM_DETECT_ID") + " Assign end : " + srouteids.toString().substring(1));

//                            tmpWorkInfoTo = new WorkInfo();
//                            tmpWorkInfoTo.hashmap.putAll(tmpWorkInfo);
//                            tmpWorkInfoTo.hashmap.put("__EX_TERM__", String.valueOf(wendtime - wstarttime));

//                            if(workNum == 1)
//                            {
//                                pGIItemRuleQueue1.addWorkItem(tmpWorkInfoTo);
//                            }
//                            else
//                            {
//                                pGIItemRuleQueue2.addWorkItem(tmpWorkInfoTo);
//                            }
                        }
                        arrFlowId1.clear();   arrFlowId1 = null;
                        arrFlowId2.clear();   arrFlowId2 = null;
                        arrFlowId21.clear();  arrFlowId21 = null;

                        //Assign Producing ť�� ������ ����
                        assignQueue.addProdQueueItem(bjob.toString());

                    }

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
    //��������ɸ���� �˶� ����ִ� ������ �ð��� ī��Ʈ ����
    private void setAutoAlarmInfo() {
    }
    //ķ���κ� ����ä�κ� ROUTE_ID ����
    private void setCampRouteId() {
        String flow_chan_id = "";
        String routeids = "", campids = "", flowids = "";
        hashFlowChan_RouteIds.clear();
        hashFlowChan_CampIds.clear();
        hashFlowChan_FlowIds.clear();
        hashFlowId_CampId.clear();
        String qry = " SELECT REAL_FLOW_ID, DETC_CHAN_CD, DETC_ROUTE_ID, CAMP_ID"
                   + " FROM R_FLOW_DETC_ROUTE "
                   + " ORDER BY REAL_FLOW_ID, DETC_CHAN_CD, DETC_ROUTE_ID ";
        List<Map<String, Object>> maps = H2JdbcTemplate.queryForList(qry);
        for(int i = 0 ; i < maps.size() ; i++){
            hashFlowId_CampId.put(maps.get(i).get("REAL_FLOW_ID").toString(), maps.get(i).get("CAMP_ID").toString());
            if(routeids.length() > 1 && (!flow_chan_id.equals(maps.get(i).get("REAL_FLOW_ID").toString() + maps.get(i).get("DETC_CHAN_CD").toString())) )
            {
                hashFlowChan_RouteIds.put(flow_chan_id, routeids.substring(1));
                routeids = "";

                hashFlowChan_FlowIds.put(flow_chan_id, flowids.substring(1));
                flowids = "";

                hashFlowChan_CampIds.put(flow_chan_id, campids.substring(1));
                campids = "";
            }
            flow_chan_id = maps.get(i).get("REAL_FLOW_ID").toString() + maps.get(i).get("DETC_CHAN_CD").toString();
            routeids += "," + maps.get(i).get("DETC_ROUTE_ID").toString();
            flowids += "," + maps.get(i).get("REAL_FLOW_ID").toString();
            campids += "," + maps.get(i).get("CAMP_ID").toString();
        }
        if(routeids.length() > 1)  // ���� �߰�
        {
            hashFlowChan_RouteIds.put(flow_chan_id, routeids.substring(1));
            routeids = "";

            hashFlowChan_FlowIds.put(flow_chan_id, flowids.substring(1));
            flowids = "";

            hashFlowChan_CampIds.put(flow_chan_id, campids.substring(1));
            campids = "";
        }

        //------------- �ǽð������������� ����
        hashTgetStup.clear();
        if(tgetfilteryn == true){
            qry = " SELECT REAL_FLOW_ID, TGET_KIND, TGET_ITEM_NM "
                + "   FROM R_RTGET_FLT_STUP_INFO "
                + " ORDER BY REAL_FLOW_ID ASC, TGET_KIND DESC " ;
            String flow_id = "", tgetInfo = "";
            maps.clear();
            maps = H2JdbcTemplate.queryForList(qry);
            for(int i = 0 ; i < maps.size() ; i++) {
                if(tgetInfo.length() > 1 && (!flow_id.equals(maps.get(i).get("REAL_FLOW_ID").toString())) )
                {
                    hashTgetStup.put(flow_id, tgetInfo.substring(1));
                    tgetInfo = "";
                }
                flow_id = maps.get(i).get("REAL_FLOW_ID").toString();
                tgetInfo += "," + maps.get(i).get("TGET_KIND").toString() + ";" + maps.get(i).get("TGET_ITEM_NM").toString();
            }
            if(tgetInfo.length() > 1){
                hashTgetStup.put(flow_id, tgetInfo.substring(1));
                tgetInfo = "";
            }
        }
        //-------- �ǽð�Wave���谴ü
        hashFlowId_WaveId.clear();
        hashFlowId_WaveActIds.clear();
        qry = " SELECT REAL_FLOW_ID, WAVE_CONN_OBJ_ID, WAVE_FLT_YN, WAVE_ACT_IDS"
            + " FROM R_WAVE_CONN_OBJ ";
        maps.clear();
        maps = H2JdbcTemplate.queryForList(qry);
        for(int i = 0 ; i < maps.size() ; i++){
            hashFlowId_WaveId.put(maps.get(i).get("REAL_FLOW_ID").toString().toString(), maps.get(i).get("WAVE_CONN_OBJ_ID").toString());
            hashFlowId_WaveFltYn.put(maps.get(i).get("REAL_FLOW_ID").toString(), maps.get(i).get("WAVE_FLT_YN").toString());
            hashFlowId_WaveActIds.put(maps.get(i).get("REAL_FLOW_ID").toString(), maps.get(i).get("WAVE_ACT_IDS").toString());
        }

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
