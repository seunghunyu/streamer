package com.realtime.streamer.chan;

import com.realtime.streamer.Queue.ChanExQueue;
import com.realtime.streamer.data.Camp;
import com.realtime.streamer.data.Olapp;
import com.realtime.streamer.rebminterface.Worker;
import com.realtime.streamer.service.CampService;
import com.realtime.streamer.service.ChanService;
import com.realtime.streamer.service.OlappService;
import com.realtime.streamer.util.Utility;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.json.JsonParseException;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ChanWorker implements Worker, CommandLineRunner {
    String toDate = "";
    String toMonth= "";

    int lastUpdate = 0;
    List<Camp> useDetcChanList;

    java.text.DateFormat df_YYYYMMDD = new java.text.SimpleDateFormat("yyyyMMdd");
    java.text.DateFormat df_YYYYMM = new java.text.SimpleDateFormat("yyyyMM");
    java.text.SimpleDateFormat df_yyyyMMdd = new java.text.SimpleDateFormat("yyyyMMdd");
    java.text.SimpleDateFormat df_yyyyMMddHHMMSS2 = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    java.text.DateFormat dfHHMMSS = new java.text.SimpleDateFormat("HH:mm:ss");

    HashMap<String,String> hashActOneTimeOlapp110 = new HashMap<String,String>();
    HashMap<String,String> hashActOneDayOlapp110  = new HashMap<String,String>();
    HashMap<String,String> hashCampOneTimeOlapp113 = new HashMap<String,String>();
    HashMap<String,String> hashCampOneDayOlapp113 = new HashMap<String,String>();
    HashMap<String,String> hashCampOneTimeOlapp108 = new HashMap<String,String>();

    HashMap<String,String> hashNoFatigue3 = new HashMap<String,String>();
    HashMap<String,String> hashNoFatigue99 = new HashMap<String,String>();
    HashMap<String,String> hashCampBrch = new HashMap<String,String>();

    HashMap<String,String> hashFlowId_Stat = new HashMap<String,String>();
    HashMap<String,String> hashExCampId_Stat = new HashMap<String,String>();
    HashMap<String,String> hashExCampId_ExSubId = new HashMap<String,String>();

    HashMap<String,String>  hashChanBrchCd = new HashMap<String,String>();
    HashMap<String,Integer> hashChanContRsrctTem = new HashMap<String,Integer>();
    HashMap<String,Integer> hashChanContRsrctCnt = new HashMap<String,Integer>();
    HashMap<String,Integer> hashChanContNoTime = new HashMap<String,Integer>();  //연속접촉시간제한

    int camp_brch_fatigue_day = 0;  // 캠페인유형별 중복제거 재수행 일수
    int chan_fatigue_count = 0;     // 채널 접촉횟수 제한 여부
    long elapsedTimeSum = 0;


    String cust_id = "";

    //---- 단축url
    String surv_short_url, lndg_short_url;  // , lndg_tget_add_prmtr, surv_tget_add_prmtr,
    HashMap<String,String> indexToShortUrl = new HashMap<String,String>();
    HashMap<String,String> hashActTagNm = new HashMap<String,String>();
    HashMap<String,String> hashActShortUrl = new HashMap<String,String>();
    HashMap<String,String> hashActAddPrmtr = new HashMap<String,String>();


    String exDBKind = "ORACLE";
    int olappSaveTerm = 0;

    List<Olapp> olappList = new ArrayList<>();
    List<Olapp> externalFatList = new ArrayList<>();

    long nowtime = 0;
    long clntime = 0;

    @Autowired
    CampService campService;

    @Autowired
    OlappService olappService;

    @Autowired
    ChanService chanService;


    @Autowired
    Utility utility;

    String tableDt = "";

    ChanExQueue chanExQueue = new ChanExQueue();

    @Override
    public void polling() {

    }

    @Override
    public void work() {

        int SuccessCnt = 0;
        int FailCnt = 0;
        boolean notClean = true;
        String camp_id = "", act_id = "", chan_cd ="", detc_route_id = "", ex_camp_id = "", real_flow_id = "", cust_id = "";
        String excldCd = "", exdBrch = "";
        String qry1 = "";
        String campstat = "";

        if(lastUpdate + 30 < LocalTime.now().getSecond()){
            tableDt = utility.getTableDtNum();
        }

        try{
            loop:
            while(true){

                if(chanExQueue.getChanWorkQ().size() == 0){
                    continue;
                }else if(chanExQueue.getChanWorkQ().size() > 0){

                    System.out.println("records count ::"+Integer.toString(chanExQueue.getChanWorkQ().size()));


                    String chanWorkItem = chanExQueue.getWorkQueueItem();
                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(chanWorkItem);

                    camp_id       = bjob.get("CAMP_ID").toString();
                    act_id        = bjob.get("ACT_ID").toString();
                    chan_cd       = bjob.get("CHAN_CD").toString();
                    detc_route_id = bjob.get("DETC_ROUTE_ID").toString();
                    ex_camp_id    = bjob.get("EX_CAMP_ID").toString();
                    real_flow_id  = bjob.get("REAL_FLOW_ID").toString();
                    cust_id       = bjob.get("CUST_ID").toString();

                    notClean = true;
                    campstat = (hashFlowId_Stat.get(ex_camp_id) == null ? "" : hashExCampId_Stat.get(ex_camp_id));





                    SuccessCnt++;
                    System.out.println("Clan Success count : "+SuccessCnt);

                    //Clan Producing 큐로 데이터 전달
                    chanExQueue.addProdQueueItem(bjob.toString());
                }

            }
        }catch(JsonParseException e){
            System.out.println("JsonParsing Error:::" + e.getMessage());
            //e.printStackTrace();
        }catch(Exception e) {
            System.out.println("Exception:::" + e.getMessage());
        }finally{

        }
    }


    public void setCampOlappList() throws Exception {
        hashActOneTimeOlapp110.clear();   // 110. 활동별 1회 중복제거  T:이벤트당)    //캠페인당 중복 제거등 여러개 있는 경우 처리 필요, Fatigue도 유형별 등 마찬가지로  110이 아닌 다른 변수를 설정하여 처리함.
        hashActOneDayOlapp110.clear();    // 110. 활동별 1회 중복제거  D:당일)
        hashCampOneTimeOlapp113.clear();  // 113. 캠페인별(flow) 1회 중복제거  T:이벤트당)
        hashCampOneDayOlapp113.clear();
        hashCampOneTimeOlapp108.clear();  // 108. 캠페인별(flow) 1회
        //1.중복제거 관련 정보 세팅
        for(int i = 0 ; i < olappList.size() ; i++){
            if(olappList.get(i).getOlappKindCd().equals("110") && olappList.get(i).getOlappUseObjId().equals("T")){
                hashActOneTimeOlapp110.put(olappList.get(i).getActId(), "110");
            }else if(olappList.get(i).getOlappKindCd().equals("110")){
                hashActOneDayOlapp110.put(olappList.get(i).getActId(), "110");
            }else if(olappList.get(i).getOlappKindCd().equals("113")){
                hashCampOneTimeOlapp113.put(olappList.get(i).getRealFlowId(), "113");
            }else if(olappList.get(i).getOlappKindCd().equals("108")){
                hashCampOneTimeOlapp113.put(olappList.get(i).getRealFlowId(), "108");
            }
        }
        //3. 캠페인 분류 코드 추출
        // select CAMP_ID, CAMP_BRCH from R_PLAN
        hashCampBrch.clear();
        List<Camp> campBrchList = campService.getCampBrchList();
        for(int i = 0 ; i < campBrchList.size() ; i++){
            hashCampBrch.put(campBrchList.get(i).getCampId(), campBrchList.get(i).getCampBrch());
        }

        //4. 3:유형별 중복제거 미수행 채널, 99:채널접촉횟수제한 미수행 채널 추출
        hashNoFatigue3.clear();
        hashNoFatigue99.clear();

        List<Olapp> noFatList = olappService.getNoFatActList();
        for(int i = 0 ; i < noFatList.size() ; i++){
            if(noFatList.get(i).getOlappKindCd().equals("3")){
                hashNoFatigue3.put(noFatList.get(i).getActId(), "1");
            }else if(noFatList.get(i).getOlappKindCd().equals("99")){
                hashNoFatigue99.put(noFatList.get(i).getActId(), "1");
            }
        }
        //5. 상태추출
        hashFlowId_Stat.clear();
        List<Camp> flowList = campService.getFlowStatList(df_YYYYMMDD.toString());
        for(int i = 0 ; i < flowList.size() ; i++){
            hashFlowId_Stat.put(flowList.get(i).getRealFlowId(), flowList.get(i).getStatCd());
        }
        //6. 캠페인 상태, 수행캠페인 정보 추출
        hashExCampId_Stat.clear();  // 캠페인 상태 추출
        hashExCampId_ExSubId.clear();  // 수행캠페인 정보 추출
        List<Camp> campStatList = campService.getExCampStatList(df_YYYYMMDD.toString());


        // SELECT EX_CAMP_STAT, EX_CAMP_ID, EX_SUB_ID  FROM  R_EX_CAMP WHERE EX_DT = ? order by EX_CAMP_ID asc
//        hashExCampId_Stat.put(rs1.getString("EX_CAMP_ID"), rs1.getString("EX_CAMP_STAT"));
//        hashExCampId_ExSubId.put(rs1.getString("EX_CAMP_ID"), rs1.getString("EX_SUB_ID"));
    }

    /*
        GlobalFatigue 정보 추출
     */
    private void getChanInfo(){
        hashChanBrchCd.clear();
        hashChanContRsrctTem.clear();
        hashChanContRsrctCnt.clear();
        hashChanContNoTime.clear();

        // Global Fatigue의 채널별 설정 정보 조회
        List<Olapp> fatChanInfo = olappService.findFatChanInfo();
        for(int i = 0 ; i < fatChanInfo.size() ; i++){

            hashChanBrchCd.put(fatChanInfo.get(i).getChanCd() , fatChanInfo.get(i).getChanBrchCd());
            hashChanContRsrctTem.put(fatChanInfo.get(i).getChanCd() , fatChanInfo.get(i).getContRsrctTem());
            hashChanContRsrctCnt.put(fatChanInfo.get(i).getChanCd(), fatChanInfo.get(i).getContRsrctCnt());
            hashChanContNoTime.put(fatChanInfo.get(i).getChanCd(), fatChanInfo.get(i).getContNoTime());
        }

        // 중복제거 일자 추출
        camp_brch_fatigue_day = olappService.findFatStupDay();
        //-----  채널 접촉횟수 제한 여부
        chan_fatigue_count = olappService.findFatStupCount();
    }

    //채널 발송 시간 중복제거 체크
    private boolean checkChanContNoTime(){
        boolean isclean = false;
//        PreparedStatement pstmt1 = null;
//        ResultSet rs1 = null;
//        String qry1 = "";
//        long saveTime = 0;
//        try
//        {
//            qry1 = " SELECT MAX(WORK_DTM_MIL) "
//                    + " FROM R_FATG_EX_CUST_LIST "
//                    + " where CHAN_BRCH_CD = ? "
//                    + "   and CRAT_DT = ? "
//                    + "   and CUST_ID = ? ";
//            pstmt1 = conrebm.prepareStatement(qry1);
//            pstmt1.setString(1, hashChanBrchCd.get(tmpWorkInfo.hashmap.get("CHAN_CD")));
//            pstmt1.setString(2, toDate);
//            pstmt1.setString(3, tmpWorkInfo.hashmap.get("CUST_ID"));
//            rs1 = pstmt1.executeQuery();
//            if(rs1.next()) {
//                saveTime = rs1.getLong(1);
//            }
//            if(saveTime == 0) return isclean;
//
//            long nowTime = System.currentTimeMillis()/1000;
//            int term = hashChanContNoTime.get(tmpWorkInfo.hashmap.get("CHAN_CD"));
//
//            if(nowTime < (saveTime + (term * 60))) {  // 저장된 시간보다 경과된 시간 추출하여 처리
//                isclean = true;
//            }
//
//        } catch(Exception ex)  {
//            try { conrebm.rollback(); } catch(Exception e) {};
//            ex.printStackTrace();
//        } finally {
//            if(pstmt1!= null)  { try { pstmt1.close(); } catch(Exception ex) {svrBridge.debug_println(ex.getMessage());}; }
//            if(rs1!= null)     { try { rs1.close(); }    catch(Exception ex) {svrBridge.debug_println(ex.getMessage());}; }
//        }
        return isclean;

    }
    // 99.채널 접촉횟수 제한 설정 대상이며 제한일수와 제한횟수가 있는 경우 체크
    private boolean checkChanContRsrct(JSONObject jsonObject){
        boolean isclean = false;
        int contCount = 0;

        if(hashChanContRsrctTem.get(jsonObject.get("CHAN_CD").toString()) == 1) {
            //                qry1 = " SELECT COUNT(*) "
//                        + " FROM R_FATG_EX_CUST_LIST "
//                        + " FROM R_FATG_EX_CUST_LIST "
//                        + " where CHAN_BRCH_CD = ? "
//                        + "   and CRAT_DT = ? "
//                        + "   and CUST_ID = ? ";
            contCount = olappService.getFatExCustList("", toDate,"", jsonObject.get("CUST_ID").toString(), jsonObject.get("CHAN_BRCH_CD").toString());

        }else{
            clntime = nowtime - (24*60*60*1000 * (hashChanContRsrctTem.get(jsonObject.get("CHAN_CD").toString()) -1));  // 당일 포함
//            qry1 = " SELECT COUNT(*) "
//                        + " FROM R_FATG_EX_CUST_LIST "
//                        + " where CHAN_BRCH_CD = ? "
//                        + "   and CRAT_DT between ? and ? "
//                        + "   and CUST_ID = ? ";
            contCount = olappService.getFatExCustList("", df_yyyyMMdd.format(new java.sql.Date(clntime)), toDate, jsonObject.get("CUST_ID").toString(), jsonObject.get("CHAN_BRCH_CD").toString());
        }

//        if(rs1.next()) {
//                contCount = rs1.getInt(1);
//        }

        if(contCount >= hashChanContRsrctCnt.get(jsonObject.get("CHAN_CD").toString())) isclean = true;

        return isclean;

    }

    //3.유형별 중복제거 체크
    private boolean checkCampBrch(JSONObject jsonObject){
        boolean isclean = false;
        int contCount = 0;
        if(camp_brch_fatigue_day == 1) {
            contCount = olappService.getFatExCustList(hashCampBrch.get(jsonObject.get("CAMP_ID").toString()), toDate,"", jsonObject.get("CUST_ID").toString(), "");

        }else{

            contCount = olappService.getFatExCustList(hashCampBrch.get(jsonObject.get("CAMP_ID").toString()), df_yyyyMMdd.format(new java.sql.Date(clntime)), toDate, jsonObject.get("CUST_ID").toString(), "");
        }
        if(contCount >= 1) isclean = true;

        return isclean;
    }

    private boolean checkOneTimeExCustList(JSONObject jsonObject, String campstat, String olappcd){
        boolean isOneTimeAct = false;
        if(olappcd.equals("110")){
//            String qry1 = " select 1 from R_OTIME_EX_CUST_LIST where ACT_ID = ? and CUST_ID = ? ";
        }else{
//            String qry1 = " select 1 from R_OTIME_EX_CUST_LIST where REAL_FLOW_ID = ? and CUST_ID = ? ";
        }

        return isOneTimeAct;
    }



    @Override
    public void run(String... args) throws Exception {

    }
}
