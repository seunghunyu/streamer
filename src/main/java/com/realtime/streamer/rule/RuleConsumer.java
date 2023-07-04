package com.realtime.streamer.rule;

import com.realtime.streamer.cosumer.DataConsumer;
import com.realtime.streamer.detect.GatherConsumer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.Properties;
/* [2023.07.04] 룰 처리 컨슈머
 *
 */
@Order(4)
@EnableAsync
@RequiredArgsConstructor
@Component
public class RuleConsumer implements DataConsumer, CommandLineRunner {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "RULE";
    Properties configs;
    KafkaConsumer<String, String> consumer;
    int lastUpdate = 0;


    public RuleConsumer(String address, String groupId, String topic) {
        this.Address = address;
        this.GroupId = groupId;
        this.topic = topic;
        this.lastUpdate = LocalTime.now().getSecond();

        this.configs = new Properties();
        this.configs.put("bootstrap.servers", Address);
        this.configs.put("session.timeout.ms", "10000"); // session 설정
        this.configs.put("group.id", GroupId); // 그룹아이디 설정
        this.configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //Producing을 위해 serializer 추가
        this.configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.configs.put("auto.offset.reset", "latest");
        this.configs.put("enable.auto.commit", false);
        this.configs.put("acks", "all");
        this.configs.put("block.on.buffer.full", "true");

        this.consumer = new KafkaConsumer<String, String>(this.configs);
        this.consumer.subscribe(Arrays.asList(topic)); // 구독할 topic 설정
    }

    @Override
    public void polling(Properties conf, Consumer consumer) {

    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("RuleConsumer start ::::::::::::::::::::::::::::");
        RuleConsumer ruleConsumer = new RuleConsumer("192.168.20.57:9092","test-consumer-group","RULE");
        polling(ruleConsumer.configs, ruleConsumer.consumer);
    }
}
