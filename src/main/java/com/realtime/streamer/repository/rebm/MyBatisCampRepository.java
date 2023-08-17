package com.realtime.streamer.repository.rebm;

import com.realtime.streamer.data.Camp;
import com.realtime.streamer.mappers.rebm.CampMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
@Slf4j
@RequiredArgsConstructor
@Repository
public class MyBatisCampRepository  implements CampRepository{


    private final CampMapper campMapper;

    @Override
    public List<Camp> getCampList() {
        return null;
    }

    @Override
    public Camp getCampOne(String id) {
        return campMapper.getCampOne(id);
    }

    @Override
    public Optional<Camp> findById(String id) {
        return Optional.ofNullable(campMapper.getCampOne(id));
    }

    @Override
    public Optional<Camp> findByDt(String strDt, String endDt) {
        return Optional.empty();
    }

    @Override
    public List<Camp> getDetcChanList() {
        return null;
    }

    @Override
    public List<Camp> getCampBrch() {
        return campMapper.getCampBrch();
    }

    @Override
    public List<Camp> getFlowStat(String endDt) {
        return campMapper.getFlowStat(endDt);
    }
}
