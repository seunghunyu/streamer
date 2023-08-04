package com.realtime.streamer.repository.rebm;

public interface ChanRepository {
    Integer countChanCust(String tableName, String campId, String actId, String realFlowId, String custId, String runYn);
    Integer countOTimeCust(String actId, String custId);
}
