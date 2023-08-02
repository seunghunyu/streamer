package com.realtime.streamer.service;

import com.realtime.streamer.data.Camp;
import com.realtime.streamer.data.Olapp;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
@Slf4j
@SpringBootTest
class OlappServiceTest {

    @Autowired
    OlappService olappService;

    @Test
    void getOlappUseList() {
        List<Olapp> list = olappService.getOlappUseList();
        log.info(Integer.toString(list.size()));
        for(int i = 0 ; i < list.size() ; i++){
            log.info("OlappInfo = {}",list.get(i).toString());
        }

    }

    @Test
    void getActExcldUseList(){
        String actId = "C111111111_001";
        List<Olapp> excldList = olappService.getActExcldUseList(actId);
        log.info("getExcldCondId =  {}", excldList.get(0).getExcldCondId());
    }

    @Test
    void getExtFatigueList(){
        List<Olapp> extFatList = olappService.getExternalFatList();
        log.info("getExternalFatList =  {}", extFatList.get(0).getOlappKindCd());
    }

}