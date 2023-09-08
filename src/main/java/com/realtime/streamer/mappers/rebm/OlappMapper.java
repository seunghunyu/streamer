package com.realtime.streamer.mappers.rebm;

import com.realtime.streamer.data.Olapp;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Optional;

@Mapper
public interface OlappMapper {
    List<Olapp> findAllOlapp();
    List<Olapp> findActExcldOlapp(@Param("actId") String actId);
    List<Olapp> findExternalFatList();
    List<Olapp> findNoFatigueAct();
    Integer getFatExCustList(@Param("campBrch") String campBrch, @Param("strDt") String strDt, @Param("endDt") String endDt,
                             @Param("custId") String custId, @Param("chanBrchCd") String chanBrchCd);

    Integer getChanFatgExCustList(@Param("chanBrchCd")String chanBrchCd, @Param("strDt")String strDt, @Param("endDt")String endDt, String custId);
}
