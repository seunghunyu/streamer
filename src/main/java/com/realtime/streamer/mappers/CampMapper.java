package com.realtime.streamer.mappers;

import com.realtime.streamer.data.Camp;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface CampMapper {
    Camp getCampOne(String id);
    List<Camp> getCampBrch();
    List<Camp> getFlowStat(String endDt);
}
