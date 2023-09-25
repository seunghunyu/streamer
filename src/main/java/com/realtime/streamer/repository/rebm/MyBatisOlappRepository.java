package com.realtime.streamer.repository.rebm;

import com.realtime.streamer.data.Olapp;
import com.realtime.streamer.mappers.crm.CrmOlappMapper;
import com.realtime.streamer.mappers.h2.H2OlappMapper;
import com.realtime.streamer.mappers.rebm.OlappMapper;
import com.realtime.streamer.repository.crm.CrmOlappRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Repository
public class MyBatisOlappRepository implements OlappRepository {

    private final OlappMapper olappMapper;
    private final H2OlappMapper h2OlappMapper;

    @Override
    public List<Olapp> getCampOlappList() {
        List<Olapp> olappList = olappMapper.findAllOlapp();
        return olappList;
    }

    public List<Olapp> getActExcldOlappList(String actId) {
        List<Olapp> actExcldList = olappMapper.findActExcldOlapp(actId);
        return actExcldList;
    }

    @Override
    public List<Olapp> getExternalFatList() {
        List<Olapp> externalFatList = olappMapper.findExternalFatList();
        return externalFatList;
    }

    @Override
    public List<Olapp> getNoFatigueAct() {
        List<Olapp> noFatList = olappMapper.findNoFatigueAct();
        return noFatList;
    }


    @Override
    public Integer getFatExCustList(String campBrch, String strDt, String endDt, String custId, String chanBrchCd) {
        Integer fatCount = olappMapper.getFatExCustList(campBrch, strDt, endDt, custId, chanBrchCd);
        return fatCount;
    }

    @Override
    public List<Olapp> getMemExternalFatList() {
        return h2OlappMapper.getMemExternalFatList();
    }

    @Override
    public Integer getChanFatgExCustList(String chanBrchCd, String strDt, String endDt, String custId) {
        return olappMapper.getChanFatgExCustList(chanBrchCd, strDt, endDt, custId);
    }

    @Override
    public Integer getFatCustMaxWorkTime(String chanBrchCd, String strDt, String endDt, String custId) {
        return olappMapper.getFatCustMaxWorkTime(chanBrchCd, strDt, endDt, custId );
    }


}
