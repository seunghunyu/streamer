package com.realtime.streamer.repository.rebm;


import com.realtime.streamer.data.ContGrp;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface ContGrpRepsoitory {
    void updateCratCnt(String contSetObjId);
    List<ContGrp> selectContSetObj (String contSetObjId);
    String getContSetYn(String contSetObjId, String custId);

    String getMemContSetYn(String contSetObjId, String custId);
    void contUpdate(String contSetObjId);
    void initContCount(String contSetObjId);
    void contExecUpdate(String contSetObjId);
    Integer selExCnt(String contSetObjId);
}
