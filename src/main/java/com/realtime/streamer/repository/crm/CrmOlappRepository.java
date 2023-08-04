package com.realtime.streamer.repository.crm;

import com.realtime.streamer.data.Olapp;

import java.util.List;

public interface CrmOlappRepository {
    List<Olapp> findFatChanInfo();
    Integer findFatStupDay();
    Integer findFatStupCount();
}
