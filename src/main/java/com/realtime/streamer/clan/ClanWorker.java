package com.realtime.streamer.clan;

import com.realtime.streamer.Queue.ClanExQueue;
import com.realtime.streamer.data.Camp;
import com.realtime.streamer.data.Olapp;
import com.realtime.streamer.rebminterface.DataConsumer;
import com.realtime.streamer.rebminterface.Worker;
import com.realtime.streamer.service.CampService;
import com.realtime.streamer.service.ChanService;
import com.realtime.streamer.service.OlappService;
import com.realtime.streamer.util.Utility;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.json.JsonParseException;
import org.springframework.scheduling.annotation.Async;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;

public class ClanWorker implements Worker, CommandLineRunner {



    int lastUpdate = 0;
    List<Camp> useDetcChanList;
    //신규 감지 ID
    BigDecimal newDetectId;

    java.text.DateFormat df_YYYYMMDD = new java.text.SimpleDateFormat("yyyyMMdd");
    java.text.DateFormat df_YYYYMM = new java.text.SimpleDateFormat("yyyyMM");
    java.text.SimpleDateFormat df_yyyyMMdd = new java.text.SimpleDateFormat("yyyyMMdd");
    java.text.SimpleDateFormat df_yyyyMMddHHMMSS2 = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    java.text.DateFormat dfHHMMSS = new java.text.SimpleDateFormat("HH:mm:ss");

    HashMap<String,String> hashActOneTimeOlapp110 = new HashMap<String,String>();
    HashMap<String,String> hashActOneDayOlapp110 = new HashMap<String,String>();
    HashMap<String,String> hashCampOneTimeOlapp113 = new HashMap<String,String>();

    HashMap<String,String> hashNoFatigue3 = new HashMap<String,String>();
    HashMap<String,String> hashNoFatigue99 = new HashMap<String,String>();

    HashMap<String, ArrayList<String>> hashAct_ExternalFatigue = new HashMap<String, ArrayList<String>>();
    HashMap<String,String> hashFlowId_Stat = new HashMap<String,String>();
    HashMap<String,String> hashCampBrch = new HashMap<String,String>();

    HashMap<String,String>  hashChanBrchCd = new HashMap<String,String>();
    HashMap<String,Integer> hashChanContRsrctTem = new HashMap<String,Integer>();
    HashMap<String,Integer> hashChanContRsrctCnt = new HashMap<String,Integer>();
    HashMap<String,Integer> hashChanContNoTime = new HashMap<String,Integer>();


    Integer camp_brch_fatigue_day = 0;  // 캠페인유형별 중복제거 재수행 일수
    Integer chan_fatigue_count = 0;     // 채널 접촉횟수 제한 여부

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

    ClanExQueue clanExQueue = new ClanExQueue();

    public ClanWorker() {
        System.out.println("call Clan Worker Constructor");
        this.lastUpdate = LocalTime.now().getSecond();
        setCampOlappList();
    }
    @Override
    public void polling() {

    }

    @Override
    public void work(){
        int SuccessCnt = 0;
        int FailCnt = 0;
        boolean notClean = true;
        String camp_id = "", act_id = "", chan_cd ="", detc_route_id = "", ex_camp_id = "", real_flow_id = "", cust_id = "";
        String excldCd = "", exdBrch = "";
        String qry1 = "";
        if(lastUpdate + 30 < LocalTime.now().getSecond()){
            //Olapp 정보 세팅
            setCampOlappList();
            tableDt = utility.getTableDtNum();
        }

        try{
            loop:
            while(true){

                if(clanExQueue.getClanWorkQ().size() == 0){
                    continue;
                }else if(clanExQueue.getClanWorkQ().size() > 0){

                    System.out.println("records count ::"+Integer.toString(clanExQueue.getClanWorkQ().size()));


                    String clanWorkItem = clanExQueue.getWorkQueueItem();
                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(clanWorkItem);
                    System.out.println(bjob.get("CUST_ID"));
                    camp_id       = bjob.get("CAMP_ID").toString();
                    act_id        = bjob.get("ACT_ID").toString();
                    chan_cd       = bjob.get("CHAN_CD").toString();
                    detc_route_id = bjob.get("DETC_ROUTE_ID").toString();
                    ex_camp_id    = bjob.get("EX_CAMP_ID").toString();
                    real_flow_id  = bjob.get("REAL_FLOW_ID").toString();
                    cust_id       = bjob.get("CUST_ID").toString();
                    notClean = true;
                     //1. 채널별 제외조건 수행
                    excldCd = runChanExcld(act_id);
                    //채널 제외 수행 후 성공 시 빈값 return
                    if(!excldCd.equals("")) {
                        notClean = false;
                        exdBrch = "2";
                        break;
                    }
                      //2. 110.캠페인 활동당 1일 채널 전송 1회 접촉 제한
                    if(notClean == true) {
                        if(!checkActOneDayOlapp110(camp_id , act_id, real_flow_id, cust_id)){
                            notClean = false;
                            exdBrch = "1";
                            excldCd = "110";
                        }
                    }
                     //3. 110.캠페인당 채널 전송 1회 접촉 제한
                    //select 1 from R_OTIME_EX_CUST_LIST where ACT_ID = ? and CUST_ID = ?
                    if(notClean == true) {
                        if(!checkActOneTimeOlapp110(real_flow_id,act_id, cust_id)){
                            notClean = false;
                            exdBrch = "1";
                            excldCd = "110";
                        }
                    }
                      //4. 99.Global Fatigue 채널 접촉횟수 제한
                    if(notClean == true){
                        if(hashNoFatigue99.get(act_id) == null && chan_fatigue_count > 0 && hashChanContRsrctTem.get(chan_cd) > 0 && hashChanContRsrctCnt.get(chan_cd) > 0)
                        {
                            int contCount = 0;
                              if(hashChanContRsrctTem.get(chan_cd) == 1) {
                                contCount = olappService.getFatExCustList("", "", df_yyyyMMdd.toString(), cust_id, hashChanBrchCd.get(chan_cd));
                            }else{
                                clntime = nowtime - (24*60*60*1000 * (hashChanContRsrctTem.get(chan_cd) -1));  // 당일 포함
                                contCount = olappService.getFatExCustList("", df_yyyyMMdd.format(new java.util.Date(clntime)), df_yyyyMMdd.toString(), cust_id,hashChanBrchCd.get(chan_cd));
                            }
                             if(contCount >= hashChanContRsrctCnt.get(chan_cd)) {
                                notClean = false;
                                exdBrch = "1";
                                excldCd = "99";
                            }
                        }
                    }
                      //5. 3.Global Fatigue 유형별  중복제거 체크
                    if(notClean == true) {
                         if (camp_brch_fatigue_day > 0 && hashNoFatigue3.get(act_id) == null) {
                            int contCount = 0;
                             if(camp_brch_fatigue_day == 1) {
                                contCount = olappService.getFatExCustList(hashCampBrch.get(chan_cd), "", df_yyyyMMdd.toString(), cust_id, "");
                            }else{
                                clntime = nowtime - (24*60*60*1000 * (hashChanContRsrctTem.get(chan_cd) -1));  // 당일 포함
                                contCount = olappService.getFatExCustList(hashCampBrch.get(chan_cd), df_yyyyMMdd.format(new java.util.Date(clntime)), df_yyyyMMdd.toString(), cust_id, "");
                            }
                             if(contCount >= 1) {
                                notClean = false;
                                exdBrch = "1";
                                excldCd = "3";
                            }
                        }
                    }
                    //6. 외부확장 fatigue가 설정되어 있는 경우
                    if(notClean == true && hashAct_ExternalFatigue.get(act_id) != null){
                        for(String OLAPP_KIND_CD : hashAct_ExternalFatigue.get(act_id))
                        {
                            String external_data = "";
                            JSONObject externalObj  = new JSONObject();
                             externalObj.put("CAMP_ID", camp_id);
                            externalObj.put("ACT_ID", act_id);
                            externalObj.put("CHAN_CD", chan_cd);
                            externalObj.put("EX_CAMP_ID", ex_camp_id);
                            externalObj.put("REAL_FLOW_ID", real_flow_id);
                            externalObj.put("EXTERNAL_DATA", external_data);
                              //외부 클래스 호출
    //                      com.realtime.streamer.externalRealTime.ExternalFatigue ext = new com.realtime.streamer.externalRealTime.ExternalFatigue();
    //                      if(ext.execute(externalObj) == false)
    //                      {
    //                         external_data = "확장 퍼티그 중복";
    //                         notClean = false;
    //                         exdBrch = "3";
    //                         excldCd = OLAPP_KIND_CD;
    //                         break;
    //                      } else {
    //                         external_data = "";
    //                      }
                         }
                     }
                     SuccessCnt++;
                     System.out.println("Clan Success count : "+SuccessCnt);

                     //Clan Producing 큐로 데이터 전달
                     clanExQueue.addProdQueueItem(bjob.toString());
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
    public void polling(Consumer consumer){


    }

    private boolean checkActOneTimeOlapp110(String realFlowId, String act_id, String cust_id) {
        Integer countOTime = chanService.getOTimeCustCount(realFlowId, act_id, cust_id);
        if(countOTime != null && countOTime > 0){
            return false;
        }

        return true;
    }

    private boolean checkActOneDayOlapp110(String camp_id, String act_id, String real_flow_id, String cust_id) {
        String tableName = "R_REBM_CHAN_EX_LIST_" + tableDt;
        int cnt = 0;
        if( hashActOneDayOlapp110.get(act_id) != null)
        {
            if(hashFlowId_Stat.get(real_flow_id) != null && hashFlowId_Stat.get(real_flow_id).equals("3100"))
            {
                //1.수행시점
                cnt = chanService.getSendChanCount(tableName, camp_id, act_id, real_flow_id, cust_id, "C");
            }else{
                //2.시뮬레이션용
                cnt = chanService.getSendChanCount(tableName, camp_id, act_id, real_flow_id, cust_id, "T");
            }

            if(cnt > 0){
                return false;
            }
        }
        return true;
    }


    /*
        중복제거 정보 세팅
     */
    public void setCampOlappList(){
        olappList = olappService.getOlappUseList();

        hashActOneTimeOlapp110.clear();  // 110. 활동별 1회 중복제거  T:이벤트당)    //캠페인당 중복 제거등 여러개 있는 경우 처리 필요, Fatigue도 유형별 등 마찬가지로  110이 아닌 다른 변수를 설정하여 처리함.
        hashCampOneTimeOlapp113.clear();  // 113. 캠페인별(flow) 1회 중복제거  T:이벤트당)
        hashActOneDayOlapp110.clear();

        //1.중복제거 관련 정보 세팅
        for(int i = 0 ; i < olappList.size() ; i++){
            if(olappList.get(i).getOlappKindCd().equals("110") && olappList.get(i).getOlappUseObjId().equals("T")){
                hashActOneTimeOlapp110.put(olappList.get(i).getActId(), "110");
            }else if(olappList.get(i).getOlappKindCd().equals("110")){
                hashActOneDayOlapp110.put(olappList.get(i).getActId(), "110");
            }else if(olappList.get(i).getOlappKindCd().equals("113")){
                hashCampOneTimeOlapp113.put(olappList.get(i).getRealFlowId(), "113");
            }
        }

        //2.확장 fatigue 여부 파악 ( h2 테이블 조회 )
        // SELECT ACT_ID, OLAPP_KIND_CD  FROM R_ACT_OLAPP_USE_LIST   WHERE OLAPP_KIND_CD like '2%' order by ACT_ID asc

        hashAct_ExternalFatigue.clear();
        String beforeActId = "";

        ArrayList<String> kindCds = new ArrayList<String>();
        externalFatList  =  olappService.getMemExternalFatList();

        for(int i = 0 ; i < externalFatList.size() ; i++){
            if( (!externalFatList.get(i).getActId().equals(beforeActId)) && beforeActId.length() > 1){
                ArrayList<String> saveIds = new ArrayList<String>();
                saveIds.addAll(kindCds);
                kindCds.clear();
                hashAct_ExternalFatigue.put(beforeActId, saveIds);
                beforeActId = "";
            }

            beforeActId = externalFatList.get(i).getActId();
            kindCds.add(externalFatList.get(i).getOlappKindCd());
        }
        if(beforeActId.length() > 0) {
            ArrayList<String> saveIds = new ArrayList<String>();
            saveIds.addAll(kindCds);
            kindCds.clear();
            hashAct_ExternalFatigue.put(beforeActId, saveIds);
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


    /*
        채널별 제외조건 수행
     */
    public String runChanExcld(String actId){
        String excldCondId = "";
        Boolean isExRule = false;
        List<Olapp> chanExcldList = olappService.getActExcldUseList(actId);
        for(int i = 0 ; i < chanExcldList.size() ; i++){
            //채널 제외 조건 ID 추출
            excldCondId = chanExcldList.get(i).getExcldCondId();
            isExRule = runRule(excldCondId);

            //임시 룰 수행
            if(!isExRule){
                return excldCondId;
            }

        }
        return "";
    }

    public boolean runRule(String excldcondId){
        return true;
    }


    @Async
    @Override
    public void run(String... args) throws Exception {
        System.out.println("Gather Consumer START::::::::::::::::::::::::::::::::::");
        ClanConsumer clanConsumer = new ClanConsumer();

        polling();
    }


}
