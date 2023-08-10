package com.realtime.streamer.repository.rebm;

import com.realtime.streamer.data.ContGrp;
import com.realtime.streamer.mappers.rebm.ContGrpMapper;
import com.realtime.streamer.service.ContGrpService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@RequiredArgsConstructor
public class MyBatisContGrpRepository implements ContGrpRepsoitory{

    private final ContGrpMapper contGrpMapper;

    @Override
    public void updateCratCnt(String contSetObjId) {
        contGrpMapper.updateCratCnt(contSetObjId);
    }

    @Override
    public List<ContGrp> selectContSetObj(String contSetObjId) {
        return contGrpMapper.selectContSetObj(contSetObjId);
    }
}
