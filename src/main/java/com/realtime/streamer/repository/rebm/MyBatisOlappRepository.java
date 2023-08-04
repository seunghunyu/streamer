package com.realtime.streamer.repository.rebm;

import com.realtime.streamer.data.Olapp;
import com.realtime.streamer.mappers.rebm.OlappMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Repository
public class MyBatisOlappRepository implements OlappRepository{

    private final OlappMapper olappMapper;

    @Override
    public List<Olapp> getCampOlappList() {
        List<Olapp> olappList = olappMapper.findAll();
        return olappList;
    }

    public List<Olapp> getActExcldOlappList(String actId) {
        List<Olapp> actExcldList = olappMapper.findActExcldOlapp(actId);
        return actExcldList;
    }

    @Override
    public List<Olapp> getExternalFatList() {
        List<Olapp> externalFatList = olappMapper.findExternalFatList();
        return externalFatList;
    }

    @Override
    public List<Olapp> getNoFatigueAct() {
        List<Olapp> noFatList = olappMapper.findNoFatigueAct();
        return noFatList;
    }

}
