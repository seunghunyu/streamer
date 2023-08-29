package com.realtime.streamer.mappers.h2;

import com.realtime.streamer.data.Camp;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface H2CampMapper {
    List<Camp> getExCampTMInfo(@Param("realFlowId") String realFlowId, @Param("exDt") String exDt);
}
