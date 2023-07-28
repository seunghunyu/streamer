package com.realtime.streamer.util;

import com.realtime.streamer.data.Camp;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.math.BigDecimal;
import java.time.LocalTime;
import java.util.List;
import java.util.Properties;

public class KafkaDefaultSetting {
    String Address = "192.168.20.57:9092";
    String GroupId = "test-consumer-group";
    String topic   = "TEST";
    Properties configs;

    public KafkaDefaultSetting(String Address, String GroupId) {
        this.Address = Address;
        this.GroupId = GroupId;
        this.topic = topic;

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
        this.configs.put("block.on.buffer.full", "true");

    }
}
