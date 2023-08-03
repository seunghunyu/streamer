package com.realtime.streamer.clan;

import com.realtime.streamer.cosumer.DataConsumer;
import com.realtime.streamer.data.Camp;
import com.realtime.streamer.data.Olapp;
import com.realtime.streamer.repository.JdbcTemplateCampRepository;
import com.realtime.streamer.repository.MyBatisCampRepository;
import com.realtime.streamer.repository.MyBatisChanRepository;
import com.realtime.streamer.repository.MyBatisOlappRepository;
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
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;


@EnableAsync
@RequiredArgsConstructor
//@Component
//public class GatherConsumer implements DataConsumer,ApplicationRunner {
public class ClanConsumer implements DataConsumer, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "TEST";
    Properties configs;
    KafkaConsumer<String, String> consumer;
    //    KafkaProducer<String, String> producer;
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
    HashMap<String,String> hashActOneDayOlapp110 = new HashMap<String,String>();
    HashMap<String,String> hashCampOneTimeOlapp113 = new HashMap<String,String>();

    HashMap<String,String> hashNoFatigue3 = new HashMap<String,String>();
    HashMap<String,String> hashNoFatigue99 = new HashMap<String,String>();

    HashMap<String, ArrayList<String>> hashAct_ExternalFatigue = new HashMap<String, ArrayList<String>>();
    HashMap<String,String> hashFlowId_Stat = new HashMap<String,String>();
    HashMap<String,String> hashCampBrch = new HashMap<String,String>();

    String exDBKind = "ORACLE";
    int olappSaveTerm = 0;

    List<Olapp> olappList = new ArrayList<>();
    List<Olapp> externalFatList = new ArrayList<>();

    @Autowired
    MyBatisCampRepository campRepository;

    @Autowired
    MyBatisOlappRepository olappRepository;

    @Autowired
    MyBatisChanRepository chanRepository;


    @Autowired
    Utility utility;

    String tableDt = "";

    public ClanConsumer(String address, String groupId, String topic) {
        System.out.println("call Gather Consumer Constructor");
        this.Address = address;
        this.GroupId = groupId;
        this.topic = topic;
        this.lastUpdate = LocalTime.now().getSecond();

        this.configs = new Properties();
        this.configs.put("bootstrap.servers", Address); // kafka server host 및 port
        //192.168.20.99:9092,192.168.20.100:9092,192.168.20.101:9092
        this.configs.put("session.timeout.ms", "10000"); // session 설정
        this.configs.put("group.id", GroupId); // 그룹아이디 설정
        this.configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // key deserializer
        this.configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // value deserializer
        this.configs.put("auto.offset.reset", "latest"); // earliest(처음부터 읽음) | latest(현재부터 읽음)
        this.configs.put("enable.auto.commit", false); //AutoCommit 여부

        this.configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");   // serialize 설정
        this.configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // serialize 설정

        this.configs.put("acks", "all");                         // 자신이 보낸 메시지에 대해 카프카로부터 확인을 기다리지 않습니다.
        this.configs.put("block.on.buffer.full", "true");        // 서버로 보낼 레코드를 버퍼링 할 때 사용할 수 있는 전체 메모리의 바이트수

        this.consumer = new KafkaConsumer<String, String>(this.configs);
        this.consumer.subscribe(Arrays.asList(topic)); // 구독할 topic 설정

        System.out.println("######### consturctor info");
        System.out.println("%%"+configs.get("group.id"));
        System.out.println("%%"+configs.get("bootstrap.servers"));
        System.out.println("%%"+configs.get("group.id"));
        System.out.println("%%"+configs.get("auto.offset.reset"));
        System.out.println("######### consturctor info end");
//        this.producer = new KafkaProducer<String, String>(this.configs);
        setCampOlappList(olappRepository);

        tableDt = utility.getTableDtNum();
    }

    public void polling(Properties conf, Consumer consumer){
        int SuccessCnt = 0;
        int FailCnt = 0;
        boolean notClean = true;
        String camp_id = "", act_id = "", chan_cd ="", detc_route_id = "", ex_camp_id = "", real_flow_id = "", cust_id = "";
        String excldCd = "", exdBrch = "";
        String qry1 = "";
        if(lastUpdate + 30 < LocalTime.now().getSecond()){
            //Olapp 정보 세팅
            setCampOlappList(olappRepository);
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

                    //1. 채널별 제외조건 수행
                    excldCd = runChanExcld(olappRepository, act_id);
                    //채널 제외 수행 후 성공 시 빈값 return
                    if(!excldCd.equals("")) {
                        notClean = false;
                        exdBrch = "2";
                        break;
                    }


                    //2. 110.캠페인 활동당 1일 채널 전송 1회 접촉 제한
                    if(notClean == true) {

                        checkActOneDayOlapp110(camp_id , act_id, real_flow_id, cust_id);
                       // if(rs1.next() == true) {
                            notClean = false;
                            exdBrch = "1";
                            excldCd = "110";
                        //}
                    }



                    //3. 99.Global Fatigue 채널 접촉횟수 제한


                    //4. 3.Global Fatigue 유형별  중복제거 체크

                    SuccessCnt++;
                    System.out.println("Clan Success count : "+SuccessCnt);
//                    if (SuccessCnt >= 30) { //최대 500건 get
//                        consumer.commitSync(); //commit
//                        break loop; //탈출
//                    }
                    consumer.commitSync(); //commit
                    producing(conf, bjob.toString());
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

    private boolean checkActOneDayOlapp110(String camp_id, String act_id, String real_flow_id, String cust_id) {
        String tableName = "R_REBM_CHAN_EX_LIST_" + tableDt;
        if( hashActOneDayOlapp110.get(act_id) != null)
        {
            if(hashFlowId_Stat.get(real_flow_id) != null && hashFlowId_Stat.get(real_flow_id).equals("3100"))
            {
            //1.수행시점
                chanRepository.countChanCust(tableName, camp_id, act_id, real_flow_id, cust_id, "C");
            }else{
            //2.시뮬레이션용
                chanRepository.countChanCust(tableName, camp_id, act_id, real_flow_id, cust_id, "T");
            }

            if(false){
                return false;
            }
        }
        return true;
    }


    public void producing(Properties conf, String producingData){
        //채널 발송 전 단계에서의 처리 토픽
        String chanTopic = "CHAN";
        //중복제거에 걸린 데이터 저장
        String clanSaveTopic = "CLAN_SAVE";

        int num = 0;

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(conf);;
        ProducerRecord<String, String> record, record2;
        record = new ProducerRecord<>(chanTopic, producingData);
        record2 = new ProducerRecord<>(clanSaveTopic, producingData);

        try {
            //  Thread.sleep(2000);
            System.out.println("CHAN MESSAGE PRODUCING:::::: " + producingData);
            //Rule처리로 이동하는 메시지
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("CHAN TOPIC SENDING EXCEPTION :: "+ exception.toString());
                }
            });
            System.out.println("CLAN SAVE PRODUCING:::::: " + producingData);
            //감지이력 저장으로 이동하는 메시지

            producer.send(record2, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("CLAN SAVE TOPIC SENDING EXCEPTION ::" + exception.toString());
                }
            });

        } catch (Exception e) {
            System.out.println("PRODUCING EXCEPTION ::" + e.toString());
        } finally {
            producer.flush();
        }

    }
    /*
        중복제거 정보 세팅
     */
    public void setCampOlappList(MyBatisOlappRepository repository){
        olappList = olappRepository.getCampOlappList();

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
        /*
        hashAct_ExternalFatigue.clear();
        String beforeActId = "";

        ArrayList<String> kindCds = new ArrayList<String>();
        externalFatList  = olappRepository.getExternalFatList();

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
        */

        //3. 캠페인 분류 코드 추출
        // select CAMP_ID, CAMP_BRCH from R_PLAN
        hashCampBrch.clear();
        List<Camp> campBrchList = campRepository.getCampBrch();
        for(int i = 0 ; i < campBrchList.size() ; i++){
            hashCampBrch.put(campBrchList.get(i).getCampId(), campBrchList.get(i).getCampBrch());
        }

        //4. 3:유형별 중복제거 미수행 채널, 99:채널접촉횟수제한 미수행 채널 추출
        hashNoFatigue3.clear();
        hashNoFatigue99.clear();

        List<Olapp> noFatList = olappRepository.getNoFatigueAct();
        for(int i = 0 ; i < noFatList.size() ; i++){
            if(noFatList.get(i).getOlappKindCd().equals("3")){
                hashNoFatigue3.put(noFatList.get(i).getActId(), "1");
            }else if(noFatList.get(i).getOlappKindCd().equals("99")){
                hashNoFatigue99.put(noFatList.get(i).getActId(), "1");
            }
        }
        //5. 상태추출
        hashFlowId_Stat.clear();
        List<Camp> flowList = campRepository.getFlowStat(df_YYYYMMDD.toString());
        for(int i = 0 ; i < flowList.size() ; i++){
            hashFlowId_Stat.put(flowList.get(i).getRealFlowId(), flowList.get(i).getStatCd());
        }



    }
    /*
        채널별 제외조건 수행
     */
    public String runChanExcld(MyBatisOlappRepository repository, String actId){
        String excldCondId = "";
        Boolean isExRule = false;
        List<Olapp> chanExcldList = repository.getActExcldOlappList(actId);
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
        ClanConsumer clanConsumer = new ClanConsumer("192.168.20.57:9092","test-consumer-group","TEST");
        polling(clanConsumer.configs, clanConsumer.consumer);
    }
}
