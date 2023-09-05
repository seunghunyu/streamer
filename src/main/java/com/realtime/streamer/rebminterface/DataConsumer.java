package com.realtime.streamer.rebminterface;

import org.apache.kafka.clients.consumer.Consumer;

public interface DataConsumer {
    void polling(Consumer consumer);
}
