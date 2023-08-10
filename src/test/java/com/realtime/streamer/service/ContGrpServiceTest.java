package com.realtime.streamer.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.jupiter.api.Assertions.*;
@Slf4j
@SpringBootTest
class ContGrpServiceTest {

    @Autowired
    ContGrpService contGrpService;

    @Test
    @Transactional
    void updateContSet(){
        String contSetObjId = "C230316901_004";
        if(contGrpService.selectContSetObj(contSetObjId) == null){
            log.info("contGrpService.selectContSetObj(contSetObjId) is null");
            return;
        }

        log.info("count = {}",contGrpService.selectContSetObj(contSetObjId).get(0).getContSetCratCnt());
        contGrpService.updateCratCnt(contSetObjId);
        log.info("count = {}",contGrpService.selectContSetObj(contSetObjId).get(0).getContSetCratCnt());

    }

}