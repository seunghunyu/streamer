package com.realtime.streamer.service;

import com.realtime.streamer.data.Camp;
import com.realtime.streamer.data.DetcChan;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Transactional
@SpringBootTest
class CampServiceTest {

    @Autowired
    CampService campService;

    @Test
    void findById() {
       Camp camp = campService.getCampById("C22052390M");
       log.info("campInfo = {}",camp);
    }

    @Test
    void findBrchList() {
        List<Camp> campBrchList = campService.getCampBrchList();
        for(int i = 0 ; i < campBrchList.size() ; i++){
            log.info("campID = {}, campBrch = {}", campBrchList.get(i).getCampId(), campBrchList.get(i).getCampBrch());
        }
    }
    @Test
    void getFlowStatList() {
        List<Camp> flowStatList = campService.getFlowStatList("20230803");
        for(int i = 0 ; i < flowStatList.size() ; i++){
            log.info("realFlowId = {} , statCd = {}",flowStatList.get(i).getRealFlowId(), flowStatList.get(i).getStatCd());
        }
    }

    @Test
    void getUseDetcChanList(){
        List<Camp> list = campService.getDetcChanList();
        for(int i = 0 ; i < list.size() ; i++){
            log.info("getCampId = {} , getDetcChanCd = {}", list.get(i).getCampId() , list.get(i).getDetcChanCd());
        }

    }
}