package com.realtime.streamer.mappers.rebm;

import com.realtime.streamer.data.PsnlTag;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface PsnlTagMapper {
    List<PsnlTag> getAllPsnlTagList();
}
