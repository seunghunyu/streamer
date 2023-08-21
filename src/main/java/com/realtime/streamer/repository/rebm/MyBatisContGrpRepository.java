package com.realtime.streamer.repository.rebm;

import com.realtime.streamer.data.ContGrp;
import com.realtime.streamer.mappers.h2.H2ContGrpMapper;
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
    private final H2ContGrpMapper h2ContGrpMapper;

    @Override
    public void updateCratCnt(String contSetObjId) {
        contGrpMapper.updateCratCnt(contSetObjId);
    }

    @Override
    public List<ContGrp> selectContSetObj(String contSetObjId) {
        return contGrpMapper.selectContSetObj(contSetObjId);
    }

    @Override
    public String getContSetYn(String contSetObjId, String custId) {
        return contGrpMapper.getContSetYn(contSetObjId, custId);
    }

    @Override
    public String getMemContSetYn(String contSetObjId, String custId) {
        return h2ContGrpMapper.getMemContSetYn(contSetObjId, custId);
    }

    @Override
    public void contUpdate(String contSetObjId) {
        h2ContGrpMapper.contUpdate(contSetObjId);
    }

    @Override
    public void initContCount(String contSetObjId) {
        h2ContGrpMapper.initContCount(contSetObjId);
    }

    @Override
    public void contExecUpdate(String contSetObjId) {
        h2ContGrpMapper.contExecUpdate(contSetObjId);
    }

    @Override
    public Integer selExCnt(String contSetObjId) {
        return h2ContGrpMapper.selExCnt(contSetObjId);
    }

}
