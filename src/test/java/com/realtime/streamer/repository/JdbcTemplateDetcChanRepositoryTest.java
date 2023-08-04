package com.realtime.streamer.repository;

import com.realtime.streamer.data.DetcChan;
import com.realtime.streamer.repository.rebm.JdbcTemplateDetcChanRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Transactional
@SpringBootTest
class JdbcTemplateDetcChanRepositoryTest {
    @Autowired
    JdbcTemplateDetcChanRepository jdbcTemplateDetcChanRepository;
    @Test
    void getUseDetcChanList(){
        System.out.println("getUseDetcChanList");
        List<DetcChan> detcChanList = jdbcTemplateDetcChanRepository.getUseDetcChanList();
        for(int i = 0; i < detcChanList.size() ; i++){
            System.out.println(detcChanList.get(i));
        }
    }
}