package com.realtime.streamer.service;


import com.realtime.streamer.data.Camp;
import com.realtime.streamer.mappers.CampMapper;
import com.realtime.streamer.repository.CampRepository;
import com.realtime.streamer.repository.MyBatisCampRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class CampService {

    @Autowired
    private final MyBatisCampRepository repository;

    public Camp getCampById(String id) {
        return repository.getCampOne(id);
    }
    public List<Camp> getCampBrchList() { return repository.getCampBrch(); }

    public List<Camp> getFlowStatList(String endDt) { return repository.getFlowStat(endDt); }
}
