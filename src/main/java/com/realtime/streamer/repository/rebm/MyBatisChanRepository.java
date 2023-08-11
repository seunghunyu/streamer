package com.realtime.streamer.repository.rebm;

import com.realtime.streamer.data.PsnlTag;
import com.realtime.streamer.data.Scrt;
import com.realtime.streamer.mappers.rebm.ChanMapper;
import com.realtime.streamer.mappers.rebm.PsnlTagMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Repository
public class MyBatisChanRepository implements ChanRepository{

    private final ChanMapper chanMapper;

    private final PsnlTagMapper psnlTagMapper;

    @Override
    public Integer countChanCust(String tableName, String campId, String actId, String realFlowId, String custId, String runYn) {
        return chanMapper.countChanSendCust(tableName, campId, actId, realFlowId, custId, runYn);
    }

    @Override
    public Integer countOTimeCust(String actId, String custId) {
        return chanMapper.countOTimeCust(actId, custId);
    }

    @Override
    public List<Scrt> getScrtInfo(String actId) {
        return chanMapper.getScrtInfo(actId);
    }

}
