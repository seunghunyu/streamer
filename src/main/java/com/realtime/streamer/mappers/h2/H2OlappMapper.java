package com.realtime.streamer.mappers.h2;

import com.realtime.streamer.data.Olapp;

import java.util.List;

public interface H2OlappMapper {
    List<Olapp> getMemExternalFatList();
}
