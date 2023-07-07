package com.realtime.streamer.util;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
@Slf4j
@Transactional
@SpringBootTest
class UtilityTest {
    @Autowired
    Utility utility;

    @Test
    void setRedisDetcChanList(){
        utility.setRedisDetcChanList();
//        utility.redistTest();
    }
    @Test
    void getRedisDetcChanList(){
        utility.getRedisDetcChanList();
    }

    @Test
    void setRedisDetcChanInstSqlList(){
        utility.setRedisDetcChanInstSqlList();
    }

    @Test
    void getRedisDetcChanInstSqlList(){
        utility.getRedisDetcChanInstSqlList("9001");
    }

    @Test
    void getDetcChanInstSqlList(){
        utility.getDetcChanInstSqlList("9001");
    }


}