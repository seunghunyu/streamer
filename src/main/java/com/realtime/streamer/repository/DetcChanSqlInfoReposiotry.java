package com.realtime.streamer.repository;

import com.realtime.streamer.data.DetcChanSql;

import java.util.List;

public interface DetcChanSqlInfoReposiotry {
    List<DetcChanSql> getUseDetcChanSqlList();
    String findByOne(String detcChanCd);
}
