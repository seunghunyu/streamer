package com.realtime.streamer.mappers;

import com.realtime.streamer.data.Camp;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface CampMapper {
    Camp getCampOne(String id);
}
