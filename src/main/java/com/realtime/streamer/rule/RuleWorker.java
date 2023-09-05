package com.realtime.streamer.rule;

import com.realtime.streamer.Queue.RuleExQueue;
import com.realtime.streamer.rebminterface.Worker;
import com.realtime.streamer.util.Utility;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.json.JsonParseException;

import java.time.LocalTime;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RuleWorker implements Worker, CommandLineRunner {

    int lastUpdate = 0;

    private Vector<RuleConsumer.InnerRuleWorkInfo> innerRuleWorkInfoArray =  new Vector<RuleConsumer.InnerRuleWorkInfo>();

    private Thread[] innerRuleWorkThread;
    private int      innerRuleWorkThreadCount = 0;;
    boolean isClose = false;
    int multiExecCount = 5;  //�ѹ��� ���� �� �� ���� �������� ����

    RuleExQueue ruleExQueue = new RuleExQueue();

    @Autowired
    Utility utility;

    public RuleWorker(String address, String groupId, String topic) {
        this.lastUpdate = LocalTime.now().getSecond();
    }

    @Override
    public void work() {

    }

    public static class InnerRuleWorkInfo
    {
        String            real_flow_id;
        //        WorkInfo          workinfo;
        JSONObject workinfo;
        String            resultStr;
        CountDownLatch countDownLatch;
        long              starttime;
        long              elapsedtime;
        String            detc_route_id;
    }

    private BlockingQueue<RuleConsumer.InnerRuleWorkInfo> InnerRuleWorkInfoQueue = new LinkedBlockingQueue<RuleConsumer.InnerRuleWorkInfo>();

    @Override
    public void polling() {


        int SuccessCnt = 0;
        int FailCnt = 0;
        if(lastUpdate + 7 < LocalTime.now().getSecond()){
            //logic
        }

        try{
            loop:
            while(true){
                if(ruleExQueue.getRuleWorkQ().size() == 0){
                    continue;
                }else if(ruleExQueue.getRuleWorkQ().size() > 0){
                    String ruleWorkItem = ruleExQueue.getWorkQueueItem();
                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(ruleWorkItem);

                    String routeIds = bjob.get("DETC_ROUTE_IDS") == null ? "" : bjob.get("DETC_ROUTE_IDS").toString();
                    String flowIds = bjob.get("REAL_FLOW_IDS") == null ? "" : bjob.get("REAL_FLOW_IDS").toString();
                    String campIds = bjob.get("CAMP_IDS") == null ? "" : bjob.get("CAMP_IDS").toString();

                    System.out.println(bjob.get("DETC_ROUTE_IDS").toString());
                    System.out.println(bjob.get("REAL_FLOW_IDS").toString());
                    System.out.println(bjob.get("CAMP_IDS").toString());

                    String resultStr = "";

                    String[] arr_route_id = routeIds.split(",");
                    String[] arr_flow_id = flowIds.split(",");
                    String[] arr_camp_id = campIds.split(",");
                    String work_camp_id = "";
                    String work_route_id = "";
                    String work_flow_id = "";

                    RuleConsumer.InnerRuleWorkInfo innerRuleWorkInfo = null;
                    innerRuleWorkInfo.workinfo = bjob;

                    JSONArray arrExActInfo = new JSONArray();

                    innerRuleWorkInfoArray.clear();

                    //CountDownLatch �̿��Ͽ� �Ѿ�� ķ���ο� �ش��ϴ� �����ٷ��� Rule���� ������� ������ �� ķ������ �ش��ϴ� �� ��������� ������ᰡ �Ǹ� ������ ���� ����
                    CountDownLatch countDownLatch = new CountDownLatch(arr_route_id.length);
                    for(int i=0; i<arr_route_id.length; i++)
                    {
                        work_route_id = arr_route_id[i];
                        work_flow_id = arr_flow_id[i];
                        work_camp_id = arr_camp_id[i];    //work_route_id.substring(0, work_route_id.indexOf("_"));

                        System.out.println("CountDown ::::::::::::::::::::::"  + work_route_id + ":::"+ work_flow_id + "::::" + work_camp_id);

                        if(work_camp_id != null && work_camp_id.length() > 3)
                        {
                            innerRuleWorkInfo = new RuleConsumer.InnerRuleWorkInfo();

                            //Rule ó���� �� ���� : �ΰ��̻��� �̺�Ʈ�� �������� ķ������ ���νý� ������ ���� ����� ���� ������ ����

//                            innerRuleWorkInfo.workinfo.hashmap.put("CAMP_ID", work_camp_id);
//                            innerRuleWorkInfo.workinfo.hashmap.put("EX_CAMP_ID", hashFlowId_ExCampId.get(work_flow_id));
//                            innerRuleWorkInfo.workinfo.hashmap.put("OBZ_TIME_SEC", svrBridge.getTimeSec()+"");
//                            innerRuleWorkInfo.workinfo.hashmap.put("WORK_SVR_ID", worksvrid);
//                            innerRuleWorkInfo.workinfo.hashmap.put("REAL_FLOW_ID", work_flow_id);

                            innerRuleWorkInfo.real_flow_id = work_flow_id;
                            innerRuleWorkInfo.detc_route_id = work_route_id;
                            innerRuleWorkInfo.countDownLatch = countDownLatch;
                            InnerRuleWorkInfoQueue.put(innerRuleWorkInfo);
                            innerRuleWorkInfoArray.addElement(innerRuleWorkInfo);

                        }
                    }
                    countDownLatch.await(); // �۾� ȣ��

                    SuccessCnt++;
                    System.out.println("RuleWorker :::::::::::: Success count : "+SuccessCnt + ", Fail count : "+ FailCnt);
                    ruleExQueue.addProdQueueItem(bjob.toString());
                }
            }
        }catch(JsonParseException e){
            System.out.println("JsonParsing Error:::" + e.getMessage());
            //e.printStackTrace();
        }catch(Exception e) {
            System.out.println("RULE CONSUMER Exception:::" + e.getMessage());
        }finally{

        }
    }

    private void  __InnerRuleRun(RuleConsumer.InnerRuleWorkInfo info)
    {
//        info.resultStr = svrBridge.invokeRule(info.workinfo.hashmap, classpath, getAddCampId(info.real_flow_id));
        // �� ���� ��� ���� ���� ����
        //�ӽ�

        System.out.println("__InnerRuleRun::::::::::::::::::::::::::::::::::::"+info.workinfo.toString());

        if(Integer.parseInt(info.workinfo.get("����").toString()) > 20 ){
            info.resultStr = "false";
        }

        info.countDownLatch.countDown();
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("RuleWorker start ::::::::::::::::::::::::::::");
        // ���� ���� Thread ����
        // Lambda Runnable
        Runnable workTask =
                ()->{
                    RuleConsumer.InnerRuleWorkInfo info;

                    while(isClose != true)
                    {
                        try
                        {

                            info = InnerRuleWorkInfoQueue.poll(1000, TimeUnit.MILLISECONDS);
                            if(info == null)
                                continue;

                            //�� ���� �� �ð� üũ
                            info.starttime = System.currentTimeMillis();
                            __InnerRuleRun(info);
                            info.elapsedtime = System.currentTimeMillis();

                        }
                        catch (Exception e)
                        {
                            if(e instanceof InterruptedException)
                                isClose = true;
                        }
                    }
                };

        innerRuleWorkThreadCount = multiExecCount;
        innerRuleWorkThread = new Thread[innerRuleWorkThreadCount];
        for(int i=0; i<innerRuleWorkThreadCount; i++)
        {
            innerRuleWorkThread[i] = new Thread(workTask);
            innerRuleWorkThread[i].setName("RuleWork" + i);
            innerRuleWorkThread[i].start();
        }

        polling();
    }
}
