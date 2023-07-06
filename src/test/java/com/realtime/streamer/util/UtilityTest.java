package com.realtime.streamer.util;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.jupiter.api.Assertions.*;
@Slf4j
@Transactional
@SpringBootTest
class UtilityTest {
    Utility utility = new Utility();
    @Test
    void setDetcChanList(){
        utility.setRedisDetcChanList();
    }
    @Test
    void getDetcChanList(){
        utility.getRedisDetcChanList();
    }
    @Test
    void setDetcChanInstSqlList(){
        utility.setRedisDetcChanInstSqlList();
    }
    @Test
    void getDetcChanInstSqlList(){
        utility.getRedisDetcChanInstSqlList();
    }
}