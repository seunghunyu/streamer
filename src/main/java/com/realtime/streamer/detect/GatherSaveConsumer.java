package com.realtime.streamer.detect;

import com.realtime.streamer.rebminterface.DataConsumer;
import com.realtime.streamer.data.DetcMstr;
import com.realtime.streamer.repository.rebm.JdbcTemplateHistorySaveRepository;
import com.realtime.streamer.util.Utility;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.json.JsonParseException;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;

@Order(4)
@EnableAsync
@RequiredArgsConstructor
//@Component
//public class GatherSaveConsumer implements DataConsumer, ApplicationRunner {
public class GatherSaveConsumer implements DataConsumer, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "detc-save-group";
    String topic   = "DETC_SAVE";
    Properties configs;
    KafkaConsumer<String, String> consumer;
    int lastUpdate = 0;

    @Autowired
    Utility utility;

    @Autowired
    JdbcTemplateHistorySaveRepository historySaveRepository;

    String tableDt = "";
    //������� ����ä�� ����Ʈ
    String DETC_CHAN_CD_LIST = "";
    //�������̺� ���� ����
    String INSQ_QRY = "";
    //�������̺� ���� ������
    String DETC_INST_ITEM = "";

    public GatherSaveConsumer(String address, String groupId, String topic) {
        this.Address = address;
        this.GroupId = groupId;
        this.topic = topic;
        this.lastUpdate = LocalTime.now().getSecond();
    }
    @Override
    public void polling(Consumer consumer) {
            int SuccessCnt = 0;
            int FailCnt = 0;
            String inst_Qry = "";
            List<DetcMstr> detcMstrList = new ArrayList<>();
            String detcChanSqlInfoItem;

            DetcMstr detcMstr;

            System.out.println("GATHER SAVE POLLING START@@@@@@@@@@@@@@@@@@@@");

            try{

                while(true){

                    if(lastUpdate + 6 < LocalTime.now().getSecond()){
                        //������� ����ä�� ��ȸ, �������̺� ���� ���� ��ȸ, ���� ���̺� ���� ������ ��ȸ
                        //tableDt = utility.getTableDtNum();
                        System.out.println(" TTTTTTTTTTTTTTTTTTTTTTTTT POLLING");

                    }
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500)); //�����Ͱ� ���� ��� �ִ� 0.5�� ��ٸ�
                    System.out.println("records :::" + records + ":::count ::"+Integer.toString(records.count()));

                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("!@#DETC SAVE  ::::" + record.value());

                        JSONParser parser = new JSONParser();
                        JSONObject bjob = (JSONObject)parser.parse(record.value());
                        String detcChanCd = "";
                        //����ä�� key �� ����ÿ��� ������ ����

                        if(bjob.get("DETC_CHAN_CD") != null && !bjob.get("DETC_CHAN_CD").toString().equals("")){
                            System.out.println("!@#DETC_CHAN_CD  ::::" + bjob.get("DETC_CHAN_CD").toString());
                            detcChanCd = bjob.get("DETC_CHAN_CD").toString();
                        }else{
                            System.out.println("!@#DETC_CHAN_CD  ::::" + bjob.get("DETC_CHAN_CD").toString());
                            inst_Qry = "";
                            continue;
                        }
                        //����ä�ο� �ش��ϴ� Insert �� ��������(Redis ���� ��������)
                        inst_Qry = utility.getRedisDetcChanInstSqlList(detcChanCd);
                        //Redis���� ������ �����Ͱ� ���� ��� RDB ��ȸ
                        if(inst_Qry == null || inst_Qry.equals("")){
                            inst_Qry = utility.getDetcChanInstSqlList(detcChanCd);
                        }

                        System.out.println("GATHER SAVE ::::::instQry::" + inst_Qry);

                        //����ä�ο� �ش��ϴ� Insert �� ��������(Redis ���� ��������)
                        detcChanSqlInfoItem = utility.getRedisDetcChanAddInfoItem(detcChanCd);

                        //Redis���� ������ �����Ͱ� ���� ��� RDB ��ȸ
                        if(detcChanSqlInfoItem == null || detcChanSqlInfoItem.equals("")){
                            detcChanSqlInfoItem = utility.getDetcChanAddInfoList(detcChanCd);
                        }

                        if(detcChanSqlInfoItem == null) continue;

                        System.out.println("GATHER SAVE ::::::detcChanSqlInfoItem::" + detcChanSqlInfoItem);

                        detcMstr = new DetcMstr();
                        detcMstr.setRebmDetcId(BigDecimal.valueOf(Double.parseDouble(bjob.get("REBM_DETECT_ID").toString())));
                        detcMstr.setWorkSvrNm(bjob.get("WORK_SVR_NM").toString());
                        detcMstr.setWorkSvrId(bjob.get("WORK_SVR_ID").toString());
                        detcMstr.setDetcChanCd(bjob.get("DETC_CHAN_CD").toString());
                        detcMstr.setCratMethCd("T");
                        detcMstr.setWorkDtmMil(0);
                        detcMstr.setStatCd("T");
                        detcMstr.setStopNodeId("");
                        detcMstr.setCustId(bjob.get("CUST_ID").toString());
                        detcMstr.setEvtOccrDtm("");


                        String na = "";

                        for (Object e : bjob.entrySet())  // ������ȭ ����
                        {
                            Map.Entry entry = (Map.Entry) e;
                            na = String.valueOf(entry.getKey());
                            //bjob.get(na).toString();
                            String itemUseList[] = detcChanSqlInfoItem.split(",");
                            for (int i = 0 ; i < itemUseList.length ; i++){
                                String item = bjob.get(itemUseList[i]).toString();
                                if(item.equals("NULL")){
                                    item = "";
                                }
                                if(i == 0){
                                    detcMstr.setCOL01(item);
                                }else if(i == 1){
                                    detcMstr.setCOL02(item);
                                }else if(i == 2){
                                    detcMstr.setCOL03(item);
                                }else if(i == 3){
                                    detcMstr.setCOL04(item);
                                }else if(i == 4){
                                    detcMstr.setCOL05(item);
                                }else if(i == 5){
                                    detcMstr.setCOL06(item);
                                }else if(i == 6){
                                    detcMstr.setCOL07(item);
                                }else if(i == 7){
                                    detcMstr.setCOL08(item);
                                }else if(i == 8){
                                    detcMstr.setCOL09(item);
                                }else if(i == 9){
                                    detcMstr.setCOL10(item);
                                }else continue ;
                            }
                        }

                        detcMstrList.add(detcMstr);
                        System.out.println("DETC SAVE DATA @@@@@@@@@@@@@@@@@");
                        System.out.println(detcMstr);

                        SuccessCnt++;
                        if (SuccessCnt >= 30) { //�ִ� 500�� get
                            historySaveRepository.batchInsertDetc(detcMstrList, inst_Qry);
                            System.out.println("DETC_SAVE Success count : "+SuccessCnt);
                            consumer.commitSync(); //commit
                            SuccessCnt = 0;
                            detcMstrList.clear();
                            //break loop; //Ż��
                        }
                        consumer.commitSync(); //commit
                    }
                    consumer.commitSync();//commit
                }
            }catch(JsonParseException e){
                System.out.println(e.getMessage());
                //e.printStackTrace();
            }catch(Exception e) {
                System.out.println("GATHER SAVE CONSUMING ERROR ::: "+e.getMessage());
            }finally{

            }
    }

//    @Override
//    public void run(ApplicationArguments args) throws Exception {
//        System.out.println("Gather SaveConsumer START::::::::::::::::::::::::::::::::::");
//        GatherSaveConsumer gatherSaveConsumer = new GatherSaveConsumer("192.168.20.57:9092", "detc-save-group", "DETC_SAVE");
//        polling(gatherSaveConsumer.configs, gatherSaveConsumer.consumer);
//
//    }
    @Async
    @Override
    public void run(String... args) throws Exception {
        System.out.println("Gather SaveConsumer START::::::::::::::::::::::::::::::::::");

        GatherSaveConsumer gatherSaveConsumer = new GatherSaveConsumer("192.168.20.57:9092", "test-consumer-group", "DETC_SAVE");
        gatherSaveConsumer.configs = utility.setKafkaConsumerConfigs(gatherSaveConsumer.Address, gatherSaveConsumer.GroupId);
        gatherSaveConsumer.consumer = new KafkaConsumer<String, String>(gatherSaveConsumer.configs);
        gatherSaveConsumer.consumer.subscribe(Arrays.asList(gatherSaveConsumer.topic)); // ������ topic ����

        polling(gatherSaveConsumer.consumer);

    }
}
