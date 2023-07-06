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
    void test(){
        System.out.println("qqqqqqqqqqqqqq");
    }

    @Test
    void setDetcChanList(){
        String detcChanCd = "9001";
        utility.setRedisDetcChanList(detcChanCd);
    }
    @Test
    void getDetcChanList(){
        String detcChanCd = "9001";
        utility.getRedisDetcChanList("9001");
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