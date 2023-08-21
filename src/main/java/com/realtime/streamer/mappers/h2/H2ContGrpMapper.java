package com.realtime.streamer.mappers.h2;

import com.realtime.streamer.data.ContGrp;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface H2ContGrpMapper {
    String getMemContSetYn(@Param("contSetObjId") String contSetObjId, @Param("custId") String custId);
    void contUpdate(@Param("contSetObjId") String contSetObjId);
    void contExecUpdate(@Param("contSetObjId") String contSetObjId);
    Integer selExCnt(@Param("contSetObjId") String contSetObjId);
    void initContCount(@Param("contSetObjId") String contSetObjId);

}
