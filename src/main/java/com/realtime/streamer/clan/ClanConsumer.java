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
        this.configs.put("bootstrap.servers", Address); // kafka server host �� port
        //192.168.20.99:9092,192.168.20.100:9092,192.168.20.101:9092
        this.configs.put("session.timeout.ms", "10000"); // session ����
        this.configs.put("group.id", GroupId); // �׷���̵� ����
        this.configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // key deserializer
        this.configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // value deserializer
        this.configs.put("auto.offset.reset", "latest"); // earliest(ó������ ����) | latest(������� ����)
        this.configs.put("enable.auto.commit", false); //AutoCommit ����

        this.configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");   // serialize ����
        this.configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // serialize ����

        this.configs.put("acks", "all");                         // �ڽ��� ���� �޽����� ���� ī��ī�κ��� Ȯ���� ��ٸ��� �ʽ��ϴ�.
        this.configs.put("block.on.buffer.full", "true");        // ������ ���� ���ڵ带 ���۸� �� �� ����� �� �ִ� ��ü �޸��� ����Ʈ��

        this.consumer = new KafkaConsumer<String, String>(this.configs);
        this.consumer.subscribe(Arrays.asList(topic)); // ������ topic ����

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
            //Olapp ���� ����
            setCampOlappList(olappRepository);
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
                    excldCd = runChanExcld(olappRepository, act_id);
                    //ä�� ���� ���� �� ���� �� �� return
                    if(!excldCd.equals("")) {
                        notClean = false;
                        exdBrch = "2";
                        break;
                    }


                    //2. 110.ķ���� Ȱ���� 1�� ä�� ���� 1ȸ ���� ����
                    if(notClean == true) {

                        checkActOneDayOlapp110(camp_id , act_id, real_flow_id, cust_id);
                       // if(rs1.next() == true) {
                            notClean = false;
                            exdBrch = "1";
                            excldCd = "110";
                        //}
                    }



                    //3. 99.Global Fatigue ä�� ����Ƚ�� ����


                    //4. 3.Global Fatigue ������  �ߺ����� üũ

                    SuccessCnt++;
                    System.out.println("Clan Success count : "+SuccessCnt);
//                    if (SuccessCnt >= 30) { //�ִ� 500�� get
//                        consumer.commitSync(); //commit
//                        break loop; //Ż��
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
            //1.�������
                chanRepository.countChanCust(tableName, camp_id, act_id, real_flow_id, cust_id, "C");
            }else{
            //2.�ùķ��̼ǿ�
                chanRepository.countChanCust(tableName, camp_id, act_id, real_flow_id, cust_id, "T");
            }

            if(false){
                return false;
            }
        }
        return true;
    }


    public void producing(Properties conf, String producingData){
        //ä�� �߼� �� �ܰ迡���� ó�� ����
        String chanTopic = "CHAN";
        //�ߺ����ſ� �ɸ� ������ ����
        String clanSaveTopic = "CLAN_SAVE";

        int num = 0;

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(conf);;
        ProducerRecord<String, String> record, record2;
        record = new ProducerRecord<>(chanTopic, producingData);
        record2 = new ProducerRecord<>(clanSaveTopic, producingData);

        try {
            //  Thread.sleep(2000);
            System.out.println("CHAN MESSAGE PRODUCING:::::: " + producingData);
            //Ruleó���� �̵��ϴ� �޽���
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("CHAN TOPIC SENDING EXCEPTION :: "+ exception.toString());
                }
            });
            System.out.println("CLAN SAVE PRODUCING:::::: " + producingData);
            //�����̷� �������� �̵��ϴ� �޽���

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
        �ߺ����� ���� ����
     */
    public void setCampOlappList(MyBatisOlappRepository repository){
        olappList = olappRepository.getCampOlappList();

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
        List<Camp> campBrchList = campRepository.getCampBrch();
        for(int i = 0 ; i < campBrchList.size() ; i++){
            hashCampBrch.put(campBrchList.get(i).getCampId(), campBrchList.get(i).getCampBrch());
        }

        //4. 3:������ �ߺ����� �̼��� ä��, 99:ä������Ƚ������ �̼��� ä�� ����
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
        //5. ��������
        hashFlowId_Stat.clear();
        List<Camp> flowList = campRepository.getFlowStat(df_YYYYMMDD.toString());
        for(int i = 0 ; i < flowList.size() ; i++){
            hashFlowId_Stat.put(flowList.get(i).getRealFlowId(), flowList.get(i).getStatCd());
        }



    }
    /*
        ä�κ� �������� ����
     */
    public String runChanExcld(MyBatisOlappRepository repository, String actId){
        String excldCondId = "";
        Boolean isExRule = false;
        List<Olapp> chanExcldList = repository.getActExcldOlappList(actId);
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
        ClanConsumer clanConsumer = new ClanConsumer("192.168.20.57:9092","test-consumer-group","TEST");
        polling(clanConsumer.configs, clanConsumer.consumer);
    }
}
