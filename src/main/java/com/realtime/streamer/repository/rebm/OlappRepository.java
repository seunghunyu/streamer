package com.realtime.streamer.repository.rebm;

import com.realtime.streamer.data.Olapp;

import java.util.List;
import java.util.Optional;

public interface OlappRepository {
    List<Olapp> getCampOlappList();
    List<Olapp> getActExcldOlappList(String actId);
    List<Olapp> getExternalFatList();
    List<Olapp> getNoFatigueAct();
    Integer getFatExCustList(String campBrch, String strDt, String endDt, String custId);
}
