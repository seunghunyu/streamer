package com.realtime.streamer.assign;

import com.realtime.streamer.Queue.AssignQueue;
import com.realtime.streamer.consumer.CoWorker;
import com.realtime.streamer.consumer.Worker;
import com.realtime.streamer.service.CampService;
import com.realtime.streamer.util.Utility;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.json.JsonParseException;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

@Order(4)
@EnableAsync
//@RequiredArgsConstructor
@Component
public class AssignWorker implements Worker, CommandLineRunner {

    int lastUpdate = 0;

    String table_dt, toDate, toTime;
    int seqNo = 0;
    int campCnt = 0;

    java.text.DateFormat dateFormat1 = new java.text.SimpleDateFormat("yyyyMMdd");
    java.text.DateFormat dateFormat2 = new java.text.SimpleDateFormat("HHmm");
    java.text.DateFormat dfhhmmss = new java.text.SimpleDateFormat("HHmmss");

    HashMap<String,String> hashChanFlowId = new HashMap<String,String>();
    AssignQueue assignQueue = new AssignQueue();

    @Autowired
    CampService campService;

    @Autowired
    Utility utility;

    public AssignWorker() {
        System.out.println("call Assign Worker Constructor");
        this.lastUpdate = LocalTime.now().getSecond();
    }


    @Override
    public void polling(){
        int SuccessCnt = 0;
        int FailCnt = 0;

        try{
            loop:
            while(true){

                if(lastUpdate + 7 < LocalTime.now().getSecond()){
                   // System.out.println("AssingWorkerQueue Size :::::::::::::" + this.assignQueue.getAssignWorkQ().toString()  + " not this :::" + assignQueue.getAssignWorkQ().toString());
                }

                if(assignQueue.getAssignWorkQ().size() == 0){
                    continue;
                }else if(assignQueue.getAssignWorkQ().size() > 0){
                    String assignWorkItem = assignQueue.getWorkQueueItem();
                    JSONParser parser = new JSONParser();
                    JSONObject bjob = (JSONObject)parser.parse(assignWorkItem);

                    System.out.println("AssingWorker !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + bjob.toString());

                    //Assign Producing 큐로 데이터 전달
                    assignQueue.addProdQueueItem(bjob.toString());

                }
            }
        }catch(JsonParseException e){
            System.out.println("JsonParsing Error:::" + e.getMessage());
            //e.printStackTrace();
        }catch(Exception e) {
            System.out.println("Assign Worker Exception:::" + e.getMessage());
        }finally{

        }

    }


    @Override
    public void work() {

    }

    @Async
    @Override
    public void run(String... args) throws Exception {
        System.out.println("Assign  Worker START::::::::::::::::::::::::::::::::::");
        AssignWorker assignWorker = new AssignWorker();

//        polling(assignWorker.assignQueue);
        polling();

    }


}
