package com.realtime.streamer.repository;

import com.realtime.streamer.data.Olapp;

import java.util.List;
import java.util.Optional;

public interface OlappRepository {
    List<Olapp> getCampOlappList();
    List<Olapp> getActExcldOlappList(String actId);
    List<Olapp> getExternalFatList();
    List<Olapp> getNoFatigueAct();
    List<Olapp> findFatChanInfo();
    Integer findFatStupDay();
    Integer findFatStupCount();
}
