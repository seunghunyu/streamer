package com.realtime.streamer.chan;

import com.realtime.streamer.cosumer.DataConsumer;
import com.realtime.streamer.data.Camp;
import com.realtime.streamer.data.Olapp;
import com.realtime.streamer.service.CampService;
import com.realtime.streamer.service.ChanService;
import com.realtime.streamer.service.OlappService;
import com.realtime.streamer.util.Utility;
import lombok.RequiredArgsConstructor;
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
import org.springframework.scheduling.annotation.EnableAsync;

import java.math.BigDecimal;
import java.sql.Connection;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;


@EnableAsync
@RequiredArgsConstructor
//@Component
//public class GatherConsumer implements DataConsumer,ApplicationRunner {
public class ChanConsumer implements DataConsumer, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "REALCHAN";

    Properties consumerConfigs, producerConfigs;

    KafkaConsumer<String, String> consumer;
    KafkaProducer<String, String> producer;
//    ProducerRecord<String, String> record, record2;
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
    HashMap<String,String> hashActOneDayOlapp110  = new HashMap<String,String>();
    HashMap<String,String> hashCampOneTimeOlapp113 = new HashMap<String,String>();
    HashMap<String,String> hashCampOneDayOlapp113 = new HashMap<String,String>();
    HashMap<String,String> hashCampOneTimeOlapp108 = new HashMap<String,String>();

    HashMap<String,String> hashNoFatigue3 = new HashMap<String,String>();
    HashMap<String,String> hashNoFatigue99 = new HashMap<String,String>();
    HashMap<String,String> hashCampBrch = new HashMap<String,String>();

    HashMap<String,String> hashFlowId_Stat = new HashMap<String,String>();


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

    public ChanConsumer(String address, String groupId, String topic) {
        System.out.println("call Gather Consumer Constructor");
        this.Address = address;
        this.GroupId = groupId;
        this.topic = topic;
        this.lastUpdate = LocalTime.now().getSecond();
    }

    public void polling(Consumer consumer){
        int SuccessCnt = 0;
        int FailCnt = 0;
        boolean notClean = true;
        String camp_id = "", act_id = "", chan_cd ="", detc_route_id = "", ex_camp_id = "", real_flow_id = "", cust_id = "";
        String excldCd = "", exdBrch = "";
        String qry1 = "";

        if(lastUpdate + 30 < LocalTime.now().getSecond()){
            //Olapp 정보 세팅
            tableDt = utility.getTableDtNum();
        }

        try{
            loop:
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500)); //데이터가 없을 경우 최대 0.5초 기다림

                if(records.count() == 0) continue ;
                System.out.println("records count ::"+Integer.toString(records.count()));

                for (ConsumerRecord<String, String> record : records) {
                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(record.value());
                    System.out.println(bjob.get("CUST_ID"));


                    camp_id       = bjob.get("CAMP_ID").toString();
                    act_id        = bjob.get("ACT_ID").toString();
                    chan_cd       = bjob.get("CHAN_CD").toString();
                    detc_route_id = bjob.get("DETC_ROUTE_ID").toString();
                    ex_camp_id    = bjob.get("EX_CAMP_ID").toString();
                    real_flow_id  = bjob.get("REAL_FLOW_ID").toString();
                    cust_id       = bjob.get("CUST_ID").toString();

                    notClean = true;


                    SuccessCnt++;
                    System.out.println("Clan Success count : "+SuccessCnt);

                    consumer.commitSync(); //commit
                    producing(bjob.toString(), notClean);
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

    public void producing(String producingData, boolean notClean){
        //채널 발송 전 단계에서의 처리 토픽
        String chanTopic = "CHAN";
        //중복제거에 걸린 데이터 저장
        String clanSaveTopic = "CLAN_SAVE";

        int num = 0;

        ProducerRecord<String, String> record, record2;
        record = new ProducerRecord<>(chanTopic, producingData);
        record2 = new ProducerRecord<>(clanSaveTopic, producingData);

        try {

            //중복제거 대상이 아닌 경우
            if(!notClean) {
                System.out.println("CHAN MESSAGE PRODUCING:::::: " + producingData);
                //채널 전송으로 이동하는 메시지
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("CHAN TOPIC SENDING EXCEPTION :: " + exception.toString());
                    }
                });
                //중복제거 대상인 경우
            }else {
                System.out.println("CLAN SAVE PRODUCING:::::: " + producingData);
                //중복제거 이력 저장으로 이동하는 메시지

                producer.send(record2, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("CLAN SAVE TOPIC SENDING EXCEPTION ::" + exception.toString());
                    }
                });
            }
        } catch (Exception e) {
            System.out.println("Clan Consumer PRODUCING EXCEPTION ::" + e.toString());
        } finally {
            producer.flush();
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


    @Async
    @Override
    public void run(String... args) throws Exception {
        System.out.println("ChanConsumer Consumer START::::::::::::::::::::::::::::::::::");
        ChanConsumer chanConsumer = new ChanConsumer("192.168.20.57:9092","test-consumer-group","REALCHAN");

        chanConsumer.consumerConfigs = utility.setKafkaConsumerConfigs(chanConsumer.Address, chanConsumer.GroupId);
        chanConsumer.consumer = new KafkaConsumer<String, String>(chanConsumer.consumerConfigs);
        chanConsumer.consumer.subscribe(Arrays.asList(chanConsumer.topic)); // 구독할 topic 설정

        chanConsumer.producerConfigs = utility.setKafkaProducerConfigs(chanConsumer.Address);
        chanConsumer.producer = new KafkaProducer<String, String>(chanConsumer.producerConfigs);

        chanConsumer.tableDt = utility.getTableDtNum();
        polling(chanConsumer.consumer);
    }
}
