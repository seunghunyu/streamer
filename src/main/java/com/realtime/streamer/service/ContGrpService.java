package com.realtime.streamer.service;

import com.realtime.streamer.data.ContGrp;
import com.realtime.streamer.repository.rebm.MyBatisContGrpRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ContGrpService {

    @Autowired
    MyBatisContGrpRepository contGrpRepository;

    public void updateCratCnt(String contSetObjId){
        contGrpRepository.updateCratCnt(contSetObjId);
    }

    public List<ContGrp> selectContSetObj(String contSetObjId){
        return contGrpRepository.selectContSetObj(contSetObjId);
    }

    public String getContSetYn(String contSetObjId, String custId) {
        return contGrpRepository.getContSetYn(contSetObjId, custId);
    }
}
