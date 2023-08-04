package com.realtime.streamer.service;


import com.realtime.streamer.repository.rebm.MyBatisChanRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ChanService {

    @Autowired
    private final MyBatisChanRepository repository;

    public Integer getSendChanCount(String tableName, String campId, String actId, String realFlowId, String custId, String runYn) {
        return repository.countChanCust(tableName, campId, actId, realFlowId, custId, runYn);
    }

    public Integer getOTimeCustCount(String actId, String custId) {
        return repository.countOTimeCust(actId, custId);
    }
}
