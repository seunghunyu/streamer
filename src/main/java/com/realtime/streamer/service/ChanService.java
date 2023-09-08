package com.realtime.streamer.service;


import com.realtime.streamer.data.Scrt;
import com.realtime.streamer.mappers.rebm.ChanMapper;
import com.realtime.streamer.repository.rebm.MyBatisChanRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ChanService {

    @Autowired
    private final MyBatisChanRepository repository;


    public Integer getSendChanCount(String tableName, String campId, String actId, String realFlowId, String custId, String runYn) {
        return repository.countChanCust(tableName, campId, actId, realFlowId, custId, runYn);
    }

    public Integer getOTimeCustCount(String realFlowId, String actId, String custId) {
        return repository.countOTimeCust(realFlowId, actId, custId);
    }


    public List<Scrt> getScrtInfo(String actId) {
        return repository.getScrtInfo(actId);
    }
}
