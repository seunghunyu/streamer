package com.realtime.streamer.service;


import com.realtime.streamer.data.Olapp;
import com.realtime.streamer.repository.rebm.MyBatisOlappRepository;
import com.realtime.streamer.repository.crm.MyBatisCrmOlappRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/*
*
* 2023.08.01 중복제거 관련 비즈니스 로직
*
* */
@Service
@RequiredArgsConstructor
public class OlappService {

    @Autowired
    private final MyBatisOlappRepository repository;

    @Autowired
    private final MyBatisCrmOlappRepository crm_repository;

    //REBM
    public List<Olapp> getOlappUseList(){
        return repository.getCampOlappList();
    }

    public List<Olapp> getActExcldUseList(String id){
        return repository.getActExcldOlappList(id);
    }
    public List<Olapp> getExternalFatList(){
        return repository.getExternalFatList();
    }

    public List<Olapp> getNoFatActList(){ return repository.getNoFatigueAct();}

    //CRM
    public List<Olapp> findFatChanInfo(){
        return crm_repository.findFatChanInfo();
    }
    public Integer findFatStupDay() {
        return crm_repository.findFatStupDay();
    }

    public Integer findFatStupCount() {
        return crm_repository.findFatStupCount();
    }
}
