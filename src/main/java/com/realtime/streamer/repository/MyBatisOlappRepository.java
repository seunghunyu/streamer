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
