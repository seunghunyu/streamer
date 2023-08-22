package com.realtime.streamer.mappers.rebm;

import com.realtime.streamer.data.PsnlTag;
import com.realtime.streamer.data.Scrt;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface PsnlTagMapper {
    //태그정보 조회
    List<PsnlTag> getAllPsnlTagList();
    //스크립트 정보 조회
    List<Scrt> getPsnlScrtInfo(String actId);



}
