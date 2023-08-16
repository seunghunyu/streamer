package com.realtime.streamer.cosumer;

import org.apache.kafka.clients.consumer.Consumer;

import java.util.Properties;

public interface DataConsumer {
    void polling(Consumer consumer);
}
