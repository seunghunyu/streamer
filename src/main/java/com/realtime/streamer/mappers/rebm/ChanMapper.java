package com.realtime.streamer.mappers.rebm;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface ChanMapper {
    Integer countChanSendCust(@Param("tableName") String tableName, @Param("campId")String campId, @Param("actId") String actId,
                          String realFlowId, @Param("custId") String custId, String runYn);

    Integer countOTimeCust(@Param("actId") String actId, @Param("custId") String custId);

}