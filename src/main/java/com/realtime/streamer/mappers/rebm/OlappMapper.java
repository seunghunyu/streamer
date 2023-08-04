package com.realtime.streamer.mappers.rebm;

import com.realtime.streamer.data.Olapp;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Optional;

@Mapper
public interface OlappMapper {
    List<Olapp> findAll();
    List<Olapp> findActExcldOlapp(@Param("actId") String actId);
    List<Olapp> findExternalFatList();
    List<Olapp> findNoFatigueAct();
}
