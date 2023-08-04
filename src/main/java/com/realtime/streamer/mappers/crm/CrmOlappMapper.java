package com.realtime.streamer.mappers.crm;

import com.realtime.streamer.data.Olapp;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface CrmOlappMapper {
    List<Olapp> findFatChanInfo();
    Integer findFatStupDay();
    Integer findFatStupCount();
}
