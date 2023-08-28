package com.realtime.streamer.cosumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

public interface CoWorker {
    void polling(Consumer consumer, Producer producer);
}
