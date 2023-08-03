package com.realtime.streamer.service;


import com.realtime.streamer.data.Camp;
import com.realtime.streamer.repository.MyBatisCampRepository;
import com.realtime.streamer.repository.MyBatisChanRepository;
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

}
