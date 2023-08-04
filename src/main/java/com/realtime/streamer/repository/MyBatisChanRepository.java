package com.realtime.streamer.repository;

import com.realtime.streamer.mappers.ChanMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

@Slf4j
@RequiredArgsConstructor
@Repository
public class MyBatisChanRepository implements ChanRepository{

    private final ChanMapper chanMapper;


    @Override
    public Integer countChanCust(String tableName, String campId, String actId, String realFlowId, String custId, String runYn) {
        return chanMapper.countChanSendCust(tableName, campId, actId, realFlowId, custId, runYn);
    }

    @Override
    public Integer countOTimeCust(String actId, String custId) {
        return chanMapper.countOTimeCust(actId, custId);
    }
}
