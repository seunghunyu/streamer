package com.realtime.streamer.chan;

import com.realtime.streamer.cosumer.DataConsumer;
import com.realtime.streamer.data.Camp;
import com.realtime.streamer.data.ContGrp;
import com.realtime.streamer.data.Olapp;
import com.realtime.streamer.data.PsnlTag;
import com.realtime.streamer.service.*;
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
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;

@EnableAsync
@RequiredArgsConstructor
//@Component
//public class GatherConsumer implements DataConsumer,ApplicationRunner {
public class RealScrtWorker implements DataConsumer, CommandLineRunner {
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



    // 대조군 관련
    String contSelect, contUpdate, contExec, contExSel;
    HashMap<String, Integer> contRatio = new HashMap<String, Integer>();
    HashMap<String, Integer> contMaxCnt = new HashMap<String, Integer>();
    HashMap<String, Integer> contCratCnt = new HashMap<String, Integer>();
    HashMap<String, String> contContObjId = new HashMap<String, String>();
    HashMap<String, String> flowId_ExUnit = new HashMap<String, String>();
    HashMap<String, ArrayList<String>> hashAct_PsnlTagNm = new HashMap<String, ArrayList<String>>();  // 활동별 개인화 태그명

    boolean isSimul = false;
    long autoAlarmTime = 0, autoAlarmCount = 0;


    String exDBKind = "ORACLE";
    int olappSaveTerm = 0;

    List<Olapp> olappList = new ArrayList<>();
    List<Olapp> externalFatList = new ArrayList<>();

    long nowtime = 0;
    long clntime = 0;

    @Autowired
    ContGrpService contGrpService;

    @Autowired
    PsnlTagService psnlTagService;

    @Autowired
    Utility utility;

    String tableDt = "";

    public RealScrtWorker(String address, String groupId, String topic) {
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


        tableDt = utility.getTableDtNum();
    }

    public void polling(Properties conf, Consumer consumer){
        int SuccessCnt = 0;
        int FailCnt = 0;
        boolean notClean = true;
        String camp_id = "", act_id = "", chan_cd ="", detc_route_id = "", ex_camp_id = "", real_flow_id = "", cust_id = "";
        String excldCd = "", exdBrch = "";
        String cont_set_obj_id = "";
        int contSetYn = 0;
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
                    contSetYn = 0;


                    //1. 대조군 설정 여부 파악 및 대조군 여부 반환
                    cont_set_obj_id = "";
                    if(flowId_ExUnit.get(real_flow_id) != null) {
                        if(flowId_ExUnit.get(real_flow_id).equals("A"))
                            cont_set_obj_id = contContObjId.get(act_id);
                        else
                            cont_set_obj_id = contContObjId.get(real_flow_id);

                        contSetYn = getCustContSetYn(cont_set_obj_id, camp_id, cust_id);
                    }
                    bjob.put("CONT_SET_OBJ_ID", cont_set_obj_id);

                    //2.개인화 태그 추출



                    SuccessCnt++;
                    System.out.println("Clan Success count : "+SuccessCnt);
//                    if (SuccessCnt >= 30) { //최대 500건 get
//                        consumer.commitSync(); //commit
//                        break loop; //탈출
//                    }
                    consumer.commitSync(); //commit
                    producing(conf, bjob.toString(), notClean);
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

    //고객별 대조군 여부 추출
    public int getCustContSetYn(String cont_set_obj_id, String camp_id, String cust_id) throws Exception{
        int contSetYn = 0;
        String dbContSet = "";
        //메모리 DB 에서 1차적으로 저장 여부 파악
//        "SELECT CONT_SET_YN FROM R_REBM_CONT_CUST_LIST WHERE CONT_SET_OBJ_ID = ? AND CUST_ID = ? ";
//        if(!dbContSet.equals("")) {
//            contSetYn = Integer.parseInt(dbContSet);
//            return contSetYn;
//        }

        //수행 시점에만
        if(!isSimul){
            // RDB 조회 후 저장 여부 파악
            // SELECT CONT_SET_YN FROM R_REBM_CONT_CUST_LIST WHERE CONT_SET_OBJ_ID = ? AND CUST_ID = ?
            dbContSet = contGrpService.getContSetYn(cont_set_obj_id, cust_id);
            if(!dbContSet.equals("")) {
                contSetYn = Integer.parseInt(dbContSet);
                //svrBridge.debug_println("2222 = " + cont_set_obj_id + "," + contSetYn);
                return contSetYn;
            }
        }
        // 현재 생성된 고객수가 최대 고객수보다 큰 경우 수행군으로 전달
        if(contCratCnt.get(cont_set_obj_id) >= contMaxCnt.get(cont_set_obj_id))
        {
            contSetYn = 0;
            return contSetYn;
        }

        // 수행 증가
//        contExec = "UPDATE R_CONT_SET_EX_CNT SET EX_CNT = EX_CNT + 1 WHERE CONT_SET_OBJ_ID = ? ";
//
        int excnt = 1;
//        contExSel = "SELECT EX_CNT FROM R_CONT_SET_EX_CNT WHERE CONT_SET_OBJ_ID = ? ";
//
//        excnt = rsEx.getInt(1);

        // 비율에 맞게 대조군 여부 계산
        int b = 100 / contRatio.get(cont_set_obj_id);
        if((excnt % 100) % b == 0) {
            contSetYn = 1;
            //svrBridge.debug_println("CONT_SET : " + camp_id + " = "+ tmpWorkInfo.hashmap.get("REBM_DETECT_ID"));
        }


        contUpdate = "UPDATE R_REBM_CONT_SET_OBJ SET CONT_SET_CRAT_CNT = CONT_SET_CRAT_CNT + 1 WHERE CONT_SET_OBJ_ID = ? ";
        // 시뮬레이션이면 메모리에 update 수행
//        if(isSimul == true && contSetYn == 1)
//        {
//            try {
//                pstmt2 = connm.prepareStatement(contUpdate);
//                pstmt2.setString(1, cont_set_obj_id);
//                pstmt2.executeUpdate();
//                connm.commit();
//            } catch(Exception e) {
//                e.printStackTrace();
//            } finally {
//                if(pstmt2 != null) { try { pstmt2.close(); } catch(Exception ex) { };  }
//            }
//        }
//        else
        if(isSimul == false && contSetYn == 1){   // 시뮬레이션이 아니면 대조군 DB에 update 수행
            contGrpService.updateCratCnt(cont_set_obj_id);
        }
//
//        // 데이터가 너무 큰 경우 초기화
//        if(excnt > 100000000) {
//            String qry0 = "UPDATE R_CONT_SET_EX_CNT SET EX_CNT = 1 WHERE CONT_SET_OBJ_ID = ? ";
//            pstmt2 = connm.prepareStatement(qry0);
//            pstmt2.setString(1, cont_set_obj_id);
//            pstmt2.executeUpdate();
//            pstmt2.close();
//        }

        return contSetYn;
    }

    /**
     * 활동별 개인화 태그 정보 추출
     * @param
     * @param
     * @param
     */
    public void setActPsnlTagNm(){

        hashAct_PsnlTagNm.clear();
        String beforeActId = "";
//        qry1 = " select ACT_ID, PSNL_TAG_NM from R_ACT_PSNL_TAG_LIST order by ACT_ID asc ";
        List<PsnlTag> psnlTagList = psnlTagService.getAllPsnlTagList();
        ArrayList<String> psnlNms = new ArrayList<String>();

        for(int i = 0 ; i < psnlTagList.size() ; i++){
            if( (!psnlTagList.get(i).getActId().equals(beforeActId)) && beforeActId.length() > 1){
                ArrayList<String> saveIds = new ArrayList<String>();
                saveIds.addAll(psnlNms);
                psnlNms.clear();
                hashAct_PsnlTagNm.put(beforeActId, saveIds);
                beforeActId = "";
            }
            beforeActId = psnlTagList.get(i).getActId();
            psnlNms.add(psnlTagList.get(i).getPsnlTagNm().replaceAll("@", ""));
        }

        if(beforeActId.length() > 0) {
            ArrayList<String> saveIds = new ArrayList<String>();
            saveIds.addAll(psnlNms);
            psnlNms.clear();
            hashAct_PsnlTagNm.put(beforeActId, saveIds);
        }
    }



    public void producing(Properties conf, String producingData, boolean notClean){
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


    @Async
    @Override
    public void run(String... args) throws Exception {
        System.out.println("RealScrt Worker Consumer START::::::::::::::::::::::::::::::::::");
        RealScrtWorker realScrtWorker = new RealScrtWorker("192.168.20.57:9092","test-consumer-group","SCRT");
        polling(realScrtWorker.configs, realScrtWorker.consumer);
    }
}
