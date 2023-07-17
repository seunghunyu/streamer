package com.realtime.streamer.util;

import com.realtime.streamer.data.DetcChanSqlInfo;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Transactional
@SpringBootTest
class UtilityTest {
    @Autowired
    Utility utility;

    @Test
    void getTableDtNum(){
        System.out.println(utility.getTableDtNum());
    }

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

    @Test
    void getDetcChanAddInfoList(){
        String itemList = utility.getDetcChanAddInfoList("9001");

        System.out.println(itemList);
    }
    @Test
    void setRedisDetcChanAddInfoItem(){
        utility.setRedisDetcChanAddInfoItem();
        String redisDetcChanAddInfoItem = utility.getRedisDetcChanAddInfoItem("9001");
        System.out.println("Detc Chan AddInfo Item List : " + redisDetcChanAddInfoItem);
    }

    @Test
    void getRedisDetcChanAddInfoItem(){
        System.out.println("TEST @@@@@@@@@@@@@9001"+utility.getRedisDetcChanAddInfoItem("9001"));
        System.out.println("TEST @@@@@@@@@@@@@9009"+utility.getRedisDetcChanAddInfoItem("9009"));
    }
}