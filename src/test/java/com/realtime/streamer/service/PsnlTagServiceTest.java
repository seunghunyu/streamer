package com.realtime.streamer.service;

import com.realtime.streamer.data.PsnlTag;
import com.realtime.streamer.data.Scrt;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
@Slf4j
@SpringBootTest
class PsnlTagServiceTest {
    @Autowired
    PsnlTagService psnlTagService;

    @Test
    void getPsnlTagList(){
        List<PsnlTag> list = psnlTagService.getAllPsnlTagList();
        for(int i = 0 ; i < list.size() ; i++){
            log.info("psnlTag = {}",list.get(i));
        }
    }
    @Test
    void getPsnlInfoList(){
        String actId = "";
        List<Scrt> list = psnlTagService.getPsnlScrtInfo(actId);
        for(int i = 0 ; i < list.size() ; i++){
            log.info("psnlTag = {}",list.get(i));
        }
    }
    @Test
    void getH2PsnlScrtInfo(){
        String actId = "C230823902_005";
            List<Scrt> list = psnlTagService.getH2PsnlScrtInfo(actId);
            for(int i = 0 ; i < list.size() ; i++){
                log.info("psnlTag = {}",list.get(i));
            }
    }

}