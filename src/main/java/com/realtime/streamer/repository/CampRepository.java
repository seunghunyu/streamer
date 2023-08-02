package com.realtime.streamer.repository;

import com.realtime.streamer.data.Camp;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;
import java.util.Optional;


public interface CampRepository {
    List<Camp> getCampList();
    Camp getCampOne(String id);
    Optional<Camp> findById(String id);
    Optional<Camp> findByDt(String strDt, String endDt);
    List<Camp> getDetcChanList();
}
