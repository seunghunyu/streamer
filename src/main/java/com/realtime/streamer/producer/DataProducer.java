package com.realtime.streamer.producer;

import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

public interface DataProducer {
    void sendMessage(Producer producer);
}
