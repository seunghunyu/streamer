package com.realtime.streamer.repository;

import com.realtime.streamer.data.Camp;

import java.util.List;

public interface CampRepository {
    List<Camp> getCampList();
    Camp getCampOne();

}
