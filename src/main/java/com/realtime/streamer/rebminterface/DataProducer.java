package com.realtime.streamer.rebminterface;

import org.apache.kafka.clients.producer.Producer;

public interface DataProducer {
    void polling(Producer producer);
}
