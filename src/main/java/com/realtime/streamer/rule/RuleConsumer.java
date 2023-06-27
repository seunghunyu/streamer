package com.realtime.streamer.rule;

import com.realtime.streamer.cosumer.DataConsumer;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.Properties;

public class RuleConsumer implements DataConsumer {

    @Override
    public void polling(Properties conf, Consumer consumer) {

    }
}
