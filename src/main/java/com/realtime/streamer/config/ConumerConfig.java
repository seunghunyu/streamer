package com.realtime.streamer.config;

public class ConumerConfig {
    String Address = "192.168.20.77:9092";
    String GroupId = "test-consumer-group";
    String topic   = "TEST";

    public ConumerConfig(String address, String groupId, String topic) {

    }
}
