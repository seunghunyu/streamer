package com.realtime.streamer.rebminterface;

import com.realtime.streamer.Queue.AssignQueue;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

public interface Worker {
    void polling();
    void work();
}
