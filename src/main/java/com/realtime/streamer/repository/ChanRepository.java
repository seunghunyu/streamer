package com.realtime.streamer.repository;

public interface ChanRepository {
    Integer countChanCust(String tableName, String campId, String actId, String realFlowId, String custId, String runYn);
}
