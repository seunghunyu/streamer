package com.realtime.streamer.clan;

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
    Properties consumerConfigs, producerConfigs;
    KafkaConsumer<String, String> consumer;
    KafkaProducer<String, String> producer;
//    ProducerRecord<String, String> record, record2;
    int lastUpdate = 0;
    List<Camp> useDetcChanList;
    //�ű� ���� ID
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


    Integer camp_brch_fatigue_day = 0;  // ķ���������� �ߺ����� ����� �ϼ�
    Integer chan_fatigue_count = 0;     // ä�� ����Ƚ�� ���� ����

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

    public ClanConsumer(String address, String groupId, String topic) {
        System.out.println("call Gather Consumer Constructor");
        this.Address = address;
        this.GroupId = groupId;
        this.topic = topic;
        this.lastUpdate = LocalTime.now().getSecond();

        this.consumerConfigs = utility.setKafkaConsumerConfigs(this.Address, this.GroupId);
        this.consumer = new KafkaConsumer<String, String>(this.consumerConfigs);
        this.consumer.subscribe(Arrays.asList(topic)); // ������ topic ����

        this.producerConfigs = utility.setKafkaProducerConfigs(this.Address);
        this.producer = new KafkaProducer<String, String>(this.producerConfigs);

        this.tableDt = utility.getTableDtNum();

        setCampOlappList();
    }

    public void polling(Consumer consumer){
        int SuccessCnt = 0;
        int FailCnt = 0;
        boolean notClean = true;
        String camp_id = "", act_id = "", chan_cd ="", detc_route_id = "", ex_camp_id = "", real_flow_id = "", cust_id = "";
        String excldCd = "", exdBrch = "";
        String qry1 = "";
        if(lastUpdate + 30 < LocalTime.now().getSecond()){
            //Olapp ���� ����
            setCampOlappList();
            tableDt = utility.getTableDtNum();
        }

        try{
            loop:
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500)); //�����Ͱ� ���� ��� �ִ� 0.5�� ��ٸ�

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

                    //1. ä�κ� �������� ����
                    excldCd = runChanExcld(act_id);
                    //ä�� ���� ���� �� ���� �� �� return
                    if(!excldCd.equals("")) {
                        notClean = false;
                        exdBrch = "2";
                        break;
                    }


                    //2. 110.ķ���� Ȱ���� 1�� ä�� ���� 1ȸ ���� ����
                    if(notClean == true) {
                        if(!checkActOneDayOlapp110(camp_id , act_id, real_flow_id, cust_id)){
                            notClean = false;
                            exdBrch = "1";
                            excldCd = "110";
                        }
                    }

                    //3. 110.ķ���δ� ä�� ���� 1ȸ ���� ����
                    //select 1 from R_OTIME_EX_CUST_LIST where ACT_ID = ? and CUST_ID = ?
                    if(notClean == true) {
                        if(!checkActOneTimeOlapp110(act_id, cust_id)){
                            notClean = false;
                            exdBrch = "1";
                            excldCd = "110";
                        }
                    }


                    //4. 99.Global Fatigue ä�� ����Ƚ�� ����
                    if(notClean == true){
                        if(hashNoFatigue99.get(act_id) == null && chan_fatigue_count > 0 && hashChanContRsrctTem.get(chan_cd) > 0 && hashChanContRsrctCnt.get(chan_cd) > 0)
                        {
                            int contCount = 0;


                            if(hashChanContRsrctTem.get(chan_cd) == 1) {
                                contCount = olappService.getFatExCustList(hashChanBrchCd.get(chan_cd), "", df_yyyyMMdd.toString(), cust_id);
                            }else{
                              clntime = nowtime - (24*60*60*1000 * (hashChanContRsrctTem.get(chan_cd) -1));  // ���� ����
                                contCount = olappService.getFatExCustList(hashChanBrchCd.get(chan_cd), df_yyyyMMdd.format(new java.util.Date(clntime)), df_yyyyMMdd.toString(), cust_id);
                            }

                            if(contCount >= hashChanContRsrctCnt.get(chan_cd)) {
                                notClean = false;
                                exdBrch = "1";
                                excldCd = "99";
                            }
                        }
                    }


                    //5. 3.Global Fatigue ������  �ߺ����� üũ
                    if(notClean == true) {

                        if (camp_brch_fatigue_day > 0 && hashNoFatigue3.get(act_id) == null) {
                            int contCount = 0;

                            if(camp_brch_fatigue_day == 1) {
                                contCount = olappService.getFatExCustList(hashChanBrchCd.get(chan_cd), "", df_yyyyMMdd.toString(), cust_id);
                            }else{
                                clntime = nowtime - (24*60*60*1000 * (hashChanContRsrctTem.get(chan_cd) -1));  // ���� ����
                                contCount = olappService.getFatExCustList(hashChanBrchCd.get(chan_cd), df_yyyyMMdd.format(new java.util.Date(clntime)), df_yyyyMMdd.toString(), cust_id);
                            }

                            if(contCount >= 1) {
                                notClean = false;
                                exdBrch = "1";
                                excldCd = "3";
                            }
                        }
                    }
                    //6. �ܺ�Ȯ�� fatigue�� �����Ǿ� �ִ� ���
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


                          //�ܺ� Ŭ���� ȣ��
//                          com.realtime.streamer.externalRealTime.ExternalFatigue ext = new com.realtime.streamer.externalRealTime.ExternalFatigue();
//                          if(ext.execute(externalObj) == false)
//                          {
//                             external_data = "Ȯ�� ��Ƽ�� �ߺ�";
//                             notClean = false;
//                             exdBrch = "3";
//                             excldCd = OLAPP_KIND_CD;
//                             break;
//                          } else {
//                             external_data = "";
//                          }

                        }

                    }

                    SuccessCnt++;
                    System.out.println("Clan Success count : "+SuccessCnt);
//                    if (SuccessCnt >= 30) { //�ִ� 500�� get
//                        consumer.commitSync(); //commit
//                        break loop; //Ż��
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

    private boolean checkActOneTimeOlapp110(String act_id, String cust_id) {
        Integer countOTime = chanService.getOTimeCustCount(act_id, cust_id);
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
            //1.�������
                cnt = chanService.getSendChanCount(tableName, camp_id, act_id, real_flow_id, cust_id, "C");
            }else{
            //2.�ùķ��̼ǿ�
                cnt = chanService.getSendChanCount(tableName, camp_id, act_id, real_flow_id, cust_id, "T");
            }

            if(cnt > 0){
                return false;
            }
        }
        return true;
    }


    public void producing(String producingData, boolean notClean){
        //ä�� �߼� �� �ܰ迡���� ó�� ����
        String chanTopic = "CHAN";
        //�ߺ����ſ� �ɸ� ������ ����
        String clanSaveTopic = "CLAN_SAVE";

        int num = 0;

        ProducerRecord<String, String> record, record2;
        record = new ProducerRecord<>(chanTopic, producingData);
        record2 = new ProducerRecord<>(clanSaveTopic, producingData);

        try {

            //�ߺ����� ����� �ƴ� ���
            if(!notClean) {
                System.out.println("CHAN MESSAGE PRODUCING:::::: " + producingData);
                //ä�� �������� �̵��ϴ� �޽���
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("CHAN TOPIC SENDING EXCEPTION :: " + exception.toString());
                    }
                });
            //�ߺ����� ����� ���
            }else {
                System.out.println("CLAN SAVE PRODUCING:::::: " + producingData);
                //�ߺ����� �̷� �������� �̵��ϴ� �޽���

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
    /*
        �ߺ����� ���� ����
     */
    public void setCampOlappList(){
        olappList = olappService.getOlappUseList();

        hashActOneTimeOlapp110.clear();  // 110. Ȱ���� 1ȸ �ߺ�����  T:�̺�Ʈ��)    //ķ���δ� �ߺ� ���ŵ� ������ �ִ� ��� ó�� �ʿ�, Fatigue�� ������ �� ����������  110�� �ƴ� �ٸ� ������ �����Ͽ� ó����.
        hashCampOneTimeOlapp113.clear();  // 113. ķ���κ�(flow) 1ȸ �ߺ�����  T:�̺�Ʈ��)
        hashActOneDayOlapp110.clear();

        //1.�ߺ����� ���� ���� ����
        for(int i = 0 ; i < olappList.size() ; i++){
            if(olappList.get(i).getOlappKindCd().equals("110") && olappList.get(i).getOlappUseObjId().equals("T")){
                hashActOneTimeOlapp110.put(olappList.get(i).getActId(), "110");
            }else if(olappList.get(i).getOlappKindCd().equals("110")){
                hashActOneDayOlapp110.put(olappList.get(i).getActId(), "110");
            }else if(olappList.get(i).getOlappKindCd().equals("113")){
                hashCampOneTimeOlapp113.put(olappList.get(i).getRealFlowId(), "113");
            }
        }

        //2.Ȯ�� fatigue ���� �ľ� ( h2 ���̺� ��ȸ )
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

        //3. ķ���� �з� �ڵ� ����
        // select CAMP_ID, CAMP_BRCH from R_PLAN
        hashCampBrch.clear();
        List<Camp> campBrchList = campService.getCampBrchList();
        for(int i = 0 ; i < campBrchList.size() ; i++){
            hashCampBrch.put(campBrchList.get(i).getCampId(), campBrchList.get(i).getCampBrch());
        }

        //4. 3:������ �ߺ����� �̼��� ä��, 99:ä������Ƚ������ �̼��� ä�� ����
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
        //5. ��������
        hashFlowId_Stat.clear();
        List<Camp> flowList = campService.getFlowStatList(df_YYYYMMDD.toString());
        for(int i = 0 ; i < flowList.size() ; i++){
            hashFlowId_Stat.put(flowList.get(i).getRealFlowId(), flowList.get(i).getStatCd());
        }
    }
    /*
        GlobalFatigue ���� ����
     */
    private void getChanInfo(){
        hashChanBrchCd.clear();
        hashChanContRsrctTem.clear();
        hashChanContRsrctCnt.clear();
        hashChanContNoTime.clear();

        // Global Fatigue�� ä�κ� ���� ���� ��ȸ
        List<Olapp> fatChanInfo = olappService.findFatChanInfo();
        for(int i = 0 ; i < fatChanInfo.size() ; i++){

            hashChanBrchCd.put(fatChanInfo.get(i).getChanCd() , fatChanInfo.get(i).getChanBrchCd());
            hashChanContRsrctTem.put(fatChanInfo.get(i).getChanCd() , fatChanInfo.get(i).getContRsrctTem());
            hashChanContRsrctCnt.put(fatChanInfo.get(i).getChanCd(), fatChanInfo.get(i).getContRsrctCnt());
            hashChanContNoTime.put(fatChanInfo.get(i).getChanCd(), fatChanInfo.get(i).getContNoTime());
        }

        // �ߺ����� ���� ����
        camp_brch_fatigue_day = olappService.findFatStupDay();

        //-----  ä�� ����Ƚ�� ���� ����
        chan_fatigue_count = olappService.findFatStupCount();

    }


    /*
        ä�κ� �������� ����
     */
    public String runChanExcld(String actId){
        String excldCondId = "";
        Boolean isExRule = false;
        List<Olapp> chanExcldList = olappService.getActExcldUseList(actId);
        for(int i = 0 ; i < chanExcldList.size() ; i++){
            //ä�� ���� ���� ID ����
            excldCondId = chanExcldList.get(i).getExcldCondId();
            isExRule = runRule(excldCondId);

            //�ӽ� �� ����
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
        ClanConsumer clanConsumer = new ClanConsumer("192.168.20.57:9092","test-consumer-group","CLAN");
        polling(clanConsumer.consumer);
    }
}
