package com.realtime.streamer.mappers;

import com.realtime.streamer.data.Olapp;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Optional;

@Mapper
public interface OlappMapper {
//    void save(Item item);
//    void update(@Param("id") Long id, @Param("updateParam") ItemUpdateDto updateParam);
//    Optional<Item> findById(Long id);
    List<Olapp> findAll();
    List<Olapp> findActExcldOlapp(@Param("actId") String actId);
    List<Olapp> findExternalFatList();
    List<Olapp> findNoFatigueAct();

    List<Olapp> findFatChanInfo();
    Integer findFatStupDay();
    Integer findFatStupCount();

}
