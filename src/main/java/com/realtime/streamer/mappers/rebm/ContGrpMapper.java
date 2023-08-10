package com.realtime.streamer.mappers.rebm;

import com.realtime.streamer.data.ContGrp;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface ContGrpMapper {
    void updateCratCnt(@Param("contSetObjId") String contSetObjId);
    List<ContGrp> selectContSetObj (@Param("contSetObjId") String contSetObjId);
    String getContSetYn(@Param("contSetObjId") String contSetObjId, @Param("custId") String custId);
}
