package com.realtime.streamer.repository;

import com.realtime.streamer.data.Camp;
import com.realtime.streamer.data.DetcChanSqlInfo;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
@Slf4j
@Transactional
@SpringBootTest
class JdbcTemplateDetcChanSqlInfoRepositoryTest {
    @Autowired
    JdbcTemplateDetcChanSqlInfoRepository repository;
    @Test
    void getUseDetcChanSqlList(){
        List<DetcChanSqlInfo> detcChanSqlInfoList = repository.getUseDetcChanSqlList();
        for(int i = 0; i < detcChanSqlInfoList.size() ; i++){
            System.out.println(detcChanSqlInfoList.get(i));
        }
    }
}