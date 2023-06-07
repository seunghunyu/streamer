package com.realtime.streamer.repository;

import com.realtime.streamer.data.Camp;

import java.util.List;
import java.util.Optional;

public interface CampRepository {
    List<Camp> getCampList();
    Camp getCampOne();
    Optional<Camp> findById(String id);
    Optional<Camp> findByDt(String strDt, String endDt);
}
