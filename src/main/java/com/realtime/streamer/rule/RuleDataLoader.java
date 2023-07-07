package com.realtime.streamer.rule;

import com.realtime.streamer.util.Utility;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.time.LocalTime;

@Order(1)
@Component
public class RuleDataLoader implements CommandLineRunner {
    Utility utility;
    int lastUpdate = 0;

    public void polling(){
        //최초, 10초에 한번씩 DataLoad 작업 실시
        if(lastUpdate + 10 < LocalTime.now().getSecond()){
            utility.setRedisDetcChanList();
            utility.setRedisDetcChanInstSqlList();
        }
    }

    @Override
    public void run(String... args) throws Exception {
        //polling();
    }
}
