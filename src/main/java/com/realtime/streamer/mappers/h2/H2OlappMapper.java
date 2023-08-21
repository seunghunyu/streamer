package com.realtime.streamer.mappers.h2;

import com.realtime.streamer.data.Olapp;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface H2OlappMapper {
    List<Olapp> getMemExternalFatList();
}
