package com.realtime.streamer.service;

import com.realtime.streamer.data.Camp;
import com.realtime.streamer.data.Olapp;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.VisibleForTesting;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Optional;

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
    @Test
    void  findFatChanInfo(){
        List<Olapp> fatChanInfo = olappService.findFatChanInfo();
        for(int i = 0 ; i < fatChanInfo.size() ; i++){
            log.info("fatChanInfo = {}",fatChanInfo.get(i).toString());
        }
    }
    @Test
    void  findFatStupDay() {
        Integer fatStupDay = olappService.findFatStupDay();
        log.info("fatStupDay = {}",fatStupDay);
    }
    @Test
    void  findFatStupCount() {
        Integer fatStupCount = olappService.findFatStupCount();
        log.info("fatStupCount = {}",fatStupCount);
    }

    @Test
    void  getFatCustList() {
        String campBrch = "A0201";
        String strDt = "20230622";
        String endDt = "20230622";
        String custId = "C0000107262";
        Integer fatCustCount = olappService.getFatExCustList(campBrch, strDt, endDt, custId);
        log.info("fatCustCount = {}",fatCustCount);
    }

    @Test
    void getMemFatlist(){
        List<Olapp> list = olappService.getMemExternalFatList();
        log.info(Integer.toString(list.size()));
        for(int i = 0; i < list.size() ; i++){
            log.info(list.get(i).toString());
        }
    }
}