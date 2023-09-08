package com.realtime.streamer.repository.rebm;

import com.realtime.streamer.data.PsnlTag;
import com.realtime.streamer.data.Scrt;

import java.util.List;

public interface ChanRepository {
    Integer countChanCust(String tableName, String campId, String actId, String realFlowId, String custId, String runYn);
    Integer countOTimeCust(String realFlowId, String actId, String custId);
    List<Scrt> getScrtInfo(String actId);
}
