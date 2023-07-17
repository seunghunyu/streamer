package com.realtime.streamer.rule;

import com.realtime.streamer.util.Utility;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.time.LocalTime;


public class RuleDataLoader  {
    Utility utility;
    int lastUpdate = 0;

    public void polling(){
        //����, 10�ʿ� �ѹ��� DataLoad �۾� �ǽ�
        if(lastUpdate + 10 < LocalTime.now().getSecond()){
            utility.setRedisDetcChanList();
            utility.setRedisDetcChanInstSqlList();
        }
    }

}
