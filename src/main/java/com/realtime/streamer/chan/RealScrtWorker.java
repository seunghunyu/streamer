package com.realtime.streamer.chan;

import com.realtime.streamer.consumer.DataConsumer;
import com.realtime.streamer.data.*;
import com.realtime.streamer.service.*;
import com.realtime.streamer.util.ElapsedTime;
import com.realtime.streamer.util.Utility;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
//@Component
//public class GatherConsumer implements DataConsumer,ApplicationRunner {
public class RealScrtWorker implements DataConsumer, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "TEST";
    Properties consumerConfigs, producerConfig;
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
    ChanService chanService;

    @Autowired
    Utility utility;

    String tableDt = "";

    String scrt_id = "";

    public RealScrtWorker(String address, String groupId, String topic) {
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

                    HashMap<String,String> actTagInfo = new HashMap<String,String>();

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
                    JSONObject tagJobj = new JSONObject();
                    if(hashAct_PsnlTagNm.get(act_id) != null){
                        for(String tagNm : hashAct_PsnlTagNm.get(act_id)) {
                            tagJobj.put(tagNm,(bjob.get(tagNm) == null ? "" : bjob.get(tagNm)));
                        }
                    }
                    actTagInfo = (HashMap)bjob;

                    // 사용 변수 초기화
                    scrt_id = "0";
                    //3. 활동별 스크립트 정보 추출
                    List<Scrt> scrtInfo = chanService.getScrtInfo(act_id);
                    if(scrtInfo.size() > 0){
                        //scrt_id 가 비어있으면 0으로 세팅
                        scrt_id = scrtInfo.get(0).getScrtId() == null ? "0" : scrtInfo.get(0).getScrtId();
                    }
                    String sqlScrt ="";
                    //4.대상목록 부가정보를 이용하여 개인화 작업 수행
                    //5.공통개인화 정보를 이용하여 개인화 작업(DB 개인화 TAG 관리를 이용)
                    //      Csql = " SELECT SQL_SCRT1, SQL_SCRT2, SQL_SCRT3, PSNL_TAG_NM_GRP, PSNL_SQL_COLM_GRP,     DB_POOL, RPLC_VAR_NM_GRP, RPLC_VAR_TY_GRP, PTAG_SQL_ID "
                    //           + " FROM R_ACT_PSNL_SQL_SCRT  WHERE ACT_ID = ? ";
                    ElapsedTime elapse = new ElapsedTime();
                    //List<Scrt> psnlScrtInfoList = psnlTagService.getPsnlScrtInfo(act_id);
                    List<Scrt> psnlScrtInfoList = psnlTagService.getH2PsnlScrtInfo(act_id);

                    for(int i = 0 ; i < psnlScrtInfoList.size(); i++){
                        sqlScrt = psnlScrtInfoList.get(i).getSqlScrt1() + psnlScrtInfoList.get(i).getSqlScrt2() == null ? ""
                                    : psnlScrtInfoList.get(i).getSqlScrt2() + psnlScrtInfoList.get(i).getSqlScrt3() == null ? "" : psnlScrtInfoList.get(i).getSqlScrt3();

                        String tagGrp = psnlScrtInfoList.get(i).getPsnlTagNmGrp();
                        String colmGrp = psnlScrtInfoList.get(i).getPsnlSqlColmgrp();

                        String tag_grp[] = tagGrp.replaceAll("@", "").split(",");
                        String colm_grp[] = colmGrp.split(",");

                        if(sqlScrt != null && sqlScrt.length() > 2) {
                            if (psnlScrtInfoList.get(i).getRplcVarNmGrp() == null || psnlScrtInfoList.get(i).getRplcVarNmGrp() == "")
                                throw new Exception("R_ACT_PSNL_SQL_SCRT.RPLC_VAR_NM_GRP is NULL");

                            String[] repcNm = psnlScrtInfoList.get(i).getRplcVarNmGrp().split(";");
                            String[] repcTy = psnlScrtInfoList.get(i).getRplcVarTyGrp().split(";");

                            elapse.startTimer();
                            if (tag_grp.length != colm_grp.length) {
                                log.info("is unmatch tag size = " + tag_grp.length + " : column size = " + colm_grp.length);
                            }
                            //이기종 디비의 경우
                            if (!psnlScrtInfoList.get(i).getDbPool().equals("REBMDB")) {

                            } else {
                                //쿼리 수행 sqlScrt(REBMDB의 경우)

                            }
                            Object[] objArr = new Object[repcNm.length];

                            for (int j = 0; j < repcNm.length ; j++){
                                if(repcNm[i].equals("CAMP_ID")) objArr[j] = camp_id;
                                else if(repcNm[i].equals("ACT_ID")) objArr[j] = act_id;
                                else if(repcTy[i].equals("CHAR")) objArr[j] = bjob.get(repcNm[i]).toString();
                                else objArr[j] = Long.parseLong(bjob.get(repcNm[i]).toString());
                            }
                            //개인화태그 아이템화
                            List<Map<String, Object>> psnlScrtColInfo = psnlTagService.getPsnlScrtColInfo(psnlScrtInfoList.get(i).getDbPool(), sqlScrt, objArr);
                            for(int j = 0 ; j < psnlScrtColInfo.size() ; j++){
                                Map<String, Object> psnlScrtColMap = psnlScrtColInfo.get(j);


                                Iterator<String> itr = psnlScrtColMap.keySet().iterator();
                                //Object[] args = new Object[psnlScrtColMap.keySet().size()];
                                int idx = 0;
                                while (itr.hasNext()){
                                    String key = itr.next();
                                    log.info("key = {}, valueClass = {}", key, psnlScrtColMap.get(key));

                                    for(int k = 0; k < tag_grp.length ; k++)  {
                                        if(tag_grp[j] != null && colm_grp[j] != null && psnlScrtColMap.get(colm_grp[j]) != null) {
                                            if(actTagInfo.get(tag_grp[j]) != null && actTagInfo.get(tag_grp[j]).length() == 0) {
                                                actTagInfo.put(tag_grp[j], psnlScrtColMap.get(colm_grp[j]).toString());
                                            }
                                            System.out.println(tag_grp[j] + " = " + psnlScrtColMap.get(colm_grp[j]));
                                        }
                                    }
                                    idx++;
                                }
                            }


                            // 소요시간 경고 체크
                            if(autoAlarmTime > 0 && autoAlarmTime <= elapse.getElapsed())
                            {
                                utility.autoAlarmSave("R_CMN_PSNL_TAG_SQL", psnlScrtInfoList.get(i).getPtagSqlId());
                                utility = null;
                            }

                        }
                    }

                    //6.RULE수행시발생된정보를 이용하여 개인화 작업 수행, 4:DB조회아이템 개인화 통합
                    if(actTagInfo != null)
                    {
                        Iterator<String> iter = actTagInfo.keySet().iterator();
                        String na;
                        while(iter.hasNext()) {
                            na = iter.next();
                            // workinfo에 값이 없으면 값을 채워서 r_rebm_chan_ex_list_X 에 저장시 사용
                            //if(tmpWorkInfo.hashmap.get(na) == null) tmpWorkInfo.hashmap.put(na, actTagInfo.get(na));
                            if(actTagInfo.get(na) != null && actTagInfo.get(na).length() > 0) bjob.put(na, actTagInfo.get(na));
                        }
                        iter = null;
                    }


                    SuccessCnt++;
                    System.out.println("Clan Success count : "+SuccessCnt);
//                    if (SuccessCnt >= 30) { //최대 500건 get
//                        consumer.commitSync(); //commit
//                        break loop; //탈출
//                    }
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

    //고객별 대조군 여부 추출
    public int getCustContSetYn(String cont_set_obj_id, String camp_id, String cust_id) throws Exception{
        int contSetYn = 0;
        String dbContSet = "";
        //메모리 DB 에서 1차적으로 저장 여부 파악
//        "SELECT CONT_SET_YN FROM R_REBM_CONT_CUST_LIST WHERE CONT_SET_OBJ_ID = ? AND CUST_ID = ? ";
        dbContSet = contGrpService.getMemContSetYn(cont_set_obj_id, cust_id);
        if(!dbContSet.equals("")) {
            contSetYn = Integer.parseInt(dbContSet);
            return contSetYn;
        }

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
        //"UPDATE R_CONT_SET_EX_CNT SET EX_CNT = EX_CNT + 1 WHERE CONT_SET_OBJ_ID = ? ";
        contGrpService.contExecUpdate(cont_set_obj_id);
        int excnt = 1;
        //"SELECT EX_CNT FROM R_CONT_SET_EX_CNT WHERE CONT_SET_OBJ_ID = ? ";
        excnt = contGrpService.selExCnt(cont_set_obj_id);
        // 비율에 맞게 대조군 여부 계산
        int b = 100 / contRatio.get(cont_set_obj_id);
        if((excnt % 100) % b == 0) {
            contSetYn = 1;
        }

        //시뮬레이션이면 메모리에 update 수행
        //"UPDATE R_REBM_CONT_SET_OBJ SET CONT_SET_CRAT_CNT = CONT_SET_CRAT_CNT + 1 WHERE CONT_SET_OBJ_ID = ? ";
        if(isSimul == true && contSetYn == 1){
            contGrpService.contUpdate(cont_set_obj_id);
        }else if(isSimul == false && contSetYn == 1){   // 시뮬레이션이 아니면 대조군 DB에 update 수행
            contGrpService.updateCratCnt(cont_set_obj_id);
        }
//
        // 데이터가 너무 큰 경우 초기화
        if(excnt > 100000000) {
            String qry0 = "UPDATE R_CONT_SET_EX_CNT SET EX_CNT = 1 WHERE CONT_SET_OBJ_ID = ? ";
            contGrpService.initContCount(cont_set_obj_id);
        }

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
        //"select ACT_ID, PSNL_TAG_NM from R_ACT_PSNL_TAG_LIST order by ACT_ID asc ";
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



    public void producing(String producingData, boolean notClean){
        //채널 발송 전 단계에서의 처리 토픽
        String chanTopic = "REALCHAN";

        int num = 0;

        ProducerRecord<String, String> record, record2;
        record = new ProducerRecord<>(chanTopic, producingData);

        try {

            //중복제거 대상이 아닌 경우

            System.out.println("CHAN MESSAGE PRODUCING:::::: " + producingData);
            //채널 전송으로 이동하는 메시지
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("CHAN TOPIC SENDING EXCEPTION :: " + exception.toString());
                }
            });


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
        RealScrtWorker realScrtWorker = new RealScrtWorker("192.168.20.57:9092","test-consumer-group","REALSCRT");
        realScrtWorker.consumerConfigs = utility.setKafkaConsumerConfigs(realScrtWorker.Address, realScrtWorker.GroupId);
        realScrtWorker.consumer = new KafkaConsumer<String, String>(realScrtWorker.consumerConfigs);
        realScrtWorker.consumer.subscribe(Arrays.asList(realScrtWorker.topic)); // 구독할 topic 설정

        realScrtWorker.producerConfig = utility.setKafkaProducerConfigs(realScrtWorker.Address);
        realScrtWorker.producer = new KafkaProducer<String, String>(producerConfig);

        realScrtWorker.tableDt = utility.getTableDtNum();

        polling(realScrtWorker.consumer);
    }
}
