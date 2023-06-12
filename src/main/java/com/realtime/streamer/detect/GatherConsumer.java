package com.realtime.streamer.detect;

import com.realtime.streamer.data.Camp;
import com.realtime.streamer.repository.JdbcTemplateCampRepository;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalTime;
import java.util.List;
import java.util.Properties;

public class GatherConsumer {
    String Address = "192.168.20.43:9092";
    String GroupId = "test-consumer-group";
    String topic   = "test";
    Properties configs = new Properties();
    KafkaConsumer<String, String> consumer;

    int lastUpdate = 0;
    List<Camp> useDetcChanList;

    @Autowired
    JdbcTemplateCampRepository repository;

    public GatherConsumer(String address, String groupId, String topic) {
        this.Address = address;
        this.GroupId = groupId;
        this.topic = topic;

    }

    public void polling(){
        if(lastUpdate + 7 < LocalTime.now().getSecond()){
            useDetcChanList = repository.getDetcChanList();
        }



    }

}
