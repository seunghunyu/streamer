package com.realtime.streamer.repository;

import com.realtime.streamer.data.Camp;
import com.realtime.streamer.repository.rebm.JdbcTemplateCampRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Slf4j
@Transactional
@SpringBootTest
class JdbcTemplateRepositoryTest {

    @Autowired
    JdbcTemplateCampRepository repository;

    @Test
    void testConnecion(){
        System.out.println("test Connection ");
    }

    @Test
    void getCampList() {
        List<Camp> camp =  repository.getCampList();
        System.out.println("R_PLAN Count :::  "+camp.size());
        //log.info("test success");
    }

    @Test
    void getCampCount() {
        System.out.println("R_PLAN Count :::  " + repository.getCampList());
        //log.info("test success");
    }

    @Test
    void getCampOne() {
        Camp camp = repository.getCampOne("C22052390M");
        System.out.println("getCampOne ::::: "+camp.getCampName());
    }

    @Test
    void findById() {
        Optional<Camp> camp = repository.findById("C22052390M");
        System.out.println(camp);
    }

    @Test
    void findByDt() {

    }

    @Test
    void getDetcChanList(){
        List<Camp> camp = repository.getDetcChanList();
        System.out.println("수행중인 캠페인에서 사용중인 감지채널 코드 ::" + camp.get(0).getDetcChanCd());
    }

}