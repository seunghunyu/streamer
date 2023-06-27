package com.realtime.streamer.detect;

import com.realtime.streamer.cosumer.DataConsumer;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.Properties;

public class GatherSaveConsumer implements DataConsumer {

    @Override
    public void polling(Properties conf, Consumer consumer) {

    }
}
