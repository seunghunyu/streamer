package com.realtime.streamer.service;

import com.realtime.streamer.data.Camp;
import com.realtime.streamer.util.Utility;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Transactional
@SpringBootTest
class ChanServiceTest {

    @Autowired
    ChanService chanService;

    @Autowired
    Utility utility;


    @Test
    void getSendChanCount() {
        String tableName = "R_REBM_CHAN_EX_LIST_" + Integer.toString(Integer.parseInt(utility.getTableDtNum())+1);
        String campId = "C230803902";
        String actId = "C230803902_006";
        String realFlowId = "C230803902_002";
        String custId = "C0000000053";
        String runYn = "C";
        Integer sendChanCount = chanService.getSendChanCount(tableName, campId, actId, realFlowId, custId, runYn);
        System.out.println(tableName);
        log.info("sendChanCount =  {}", sendChanCount);
    }

}