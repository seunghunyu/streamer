package com.realtime.streamer.mappers.rebm;

import com.realtime.streamer.data.Scrt;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface ChanMapper {
    Integer countChanSendCust(@Param("tableName") String tableName, @Param("campId")String campId, @Param("actId") String actId,
                          String realFlowId, @Param("custId") String custId, String runYn);

    Integer countOTimeCust(@Param("realFlowId") String realFlowId, @Param("actId") String actId, @Param("custId") String custId);

    List<Scrt> getScrtInfo(@Param("actId") String actId);

}
