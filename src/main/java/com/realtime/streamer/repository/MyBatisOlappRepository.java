package com.realtime.streamer.repository;

import com.realtime.streamer.data.Camp;
import com.realtime.streamer.data.Olapp;
import com.realtime.streamer.mappers.OlappMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

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


    @Override
    public List<Olapp> findFatChanInfo() {
        List<Olapp> fatChanInfo = olappMapper.findFatChanInfo();
        return fatChanInfo;
    }

    @Override
    public Integer findFatStupDay() {
        Integer fatStupDay = olappMapper.findFatStupDay();
        return fatStupDay;
    }

    @Override
    public Integer findFatStupCount() {
        Integer fatStupCount = olappMapper.findFatStupCount();
        return fatStupCount;
    }

//    @Override
//    public void update(Long itemId, ItemUpdateDto updateParam) {
//        itemMapper.update(itemId, updateParam);
//    }
//
//    @Override
//    public Optional<Item> findById(Long id) {
//        return itemMapper.findById(id);
//    }
//
//    @Override
//    public List<Item> findAll(ItemSearchCond cond) {
//        return itemMapper.findAll(cond);
//    }
}
