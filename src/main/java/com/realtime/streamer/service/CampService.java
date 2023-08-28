package com.realtime.streamer.service;


import com.realtime.streamer.data.Camp;
import com.realtime.streamer.repository.rebm.JdbcTemplateCampRepository;
import com.realtime.streamer.repository.rebm.MyBatisCampRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class CampService {

    @Autowired
    private final MyBatisCampRepository repository;

    @Autowired
    private final JdbcTemplateCampRepository campRepository;


    public Camp getCampById(String id) {
        return repository.getCampOne(id);
    }
    public List<Camp> getCampBrchList() { return repository.getCampBrch(); }
    public List<Camp> getFlowStatList(String endDt) { return repository.getFlowStat(endDt); }
    public List<Camp> getExCampStatList(String toDate) {
        return repository.getExCampStatList(toDate);
    }


    public List<Camp> getDetcChanList(){return campRepository.getDetcChanList();}

    public List<Camp> getExCampChanInfo(String exDt) { return repository.getExCampChanInfo(exDt);}
}
