package com.realtime.streamer.consumer;

import org.apache.kafka.clients.consumer.Consumer;

public interface DataConsumer {
    void polling(Consumer consumer);
}
