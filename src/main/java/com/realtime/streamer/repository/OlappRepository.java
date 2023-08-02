package com.realtime.streamer.repository;

import com.realtime.streamer.data.Olapp;

import java.util.List;

public interface OlappRepository {
    List<Olapp> getCampOlappList();
    List<Olapp> getActExcldOlappList(String actId);
    List<Olapp> getExternalFatList();
    List<Olapp> getNoFatigueAct();
}
