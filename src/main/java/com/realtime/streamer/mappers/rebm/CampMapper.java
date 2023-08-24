package com.realtime.streamer.mappers.rebm;

import com.realtime.streamer.data.Camp;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface CampMapper {
    Camp getCampOne(String id);
    List<Camp> getCampBrch();
    List<Camp> getFlowStat(@Param("endDt") String endDt);
    List<Camp> getExCampStatList(@Param("exDt") String toDate);
}
