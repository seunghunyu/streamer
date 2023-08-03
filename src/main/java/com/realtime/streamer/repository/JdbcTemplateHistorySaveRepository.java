package com.realtime.streamer.repository;

import com.realtime.streamer.data.*;
import com.realtime.streamer.util.Utility;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Base64;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Repository
public class JdbcTemplateHistorySaveRepository implements HistorySaveRepository{
    @Autowired
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    Utility utility;

    @Override
    public boolean saveDetc(DetcMstr detcMstr, String qry) {
        String tableDt = utility.getTableDtNum();
        //String qry = "";

        jdbcTemplate.update(qry, detcMstr);
        return false;
    }

    @Override
    public boolean saveRuleS(RuleExS ruleExS, String qry) {
        return false;
    }

    @Override
    public boolean saveRuleF(RuleExF ruleExF, String qry) {
        return false;
    }

    @Override
    public boolean saveClan(ClanEx clanEx, String qry) {
        return false;
    }

    @Override
    public boolean saveChan(ChanEx chanEx, String qry) {
        return false;
    }


    @Override
    public boolean batchInsertDetc(List<DetcMstr> detcList, String qry) {
        jdbcTemplate.batchUpdate(qry, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                DetcMstr detcMstr = detcList.get(i);
                ps.setBigDecimal(1, detcMstr.getRebmDetcId());
                ps.setString(2, detcMstr.getWorkSvrNm());
                ps.setString(3, detcMstr.getWorkSvrId());
                ps.setString(4, detcMstr.getDetcChanCd());
                ps.setString(5, detcMstr.getCratMethCd());
                ps.setInt(6, detcMstr.getWorkDtmMil());
                ps.setString(7, detcMstr.getStatCd());
                ps.setString(8, detcMstr.getStopNodeId());
                ps.setString(9, detcMstr.getCustId());
                ps.setString(10, detcMstr.getEvtOccrDtm());

                ps.setString(11, detcMstr.getCOL01());
                ps.setString(12, detcMstr.getCOL02());
                ps.setString(13, detcMstr.getCOL03());
                ps.setString(14, detcMstr.getCOL04());
                ps.setString(15, detcMstr.getCOL05());
                ps.setString(16, detcMstr.getCOL06());
                ps.setString(17, detcMstr.getCOL07());
                ps.setString(18, detcMstr.getCOL08());
                ps.setString(19, detcMstr.getCOL09());
                ps.setString(20, detcMstr.getCOL10());
            }
            @Override
            public int getBatchSize() {
                return detcList.size();
            }
        });

        return true;
    }

    @Override
    public boolean batchInsertRuleS(List<RuleExS> ruleSList, String qry) {
        jdbcTemplate.batchUpdate(qry, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                RuleExS ruleExS = ruleSList.get(i);
                ps.setBigDecimal(1, ruleExS.getRebmDetcId());
                ps.setString(2, ruleExS.getExCampId());
                ps.setString(3, ruleExS.getStepId());
                ps.setString(4, ruleExS.getDetcRouteId());
                ps.setString(5, ruleExS.getWorkDtmMil());
                ps.setString(6, ruleExS.getExTerm());
                ps.setString(7, ruleExS.getCampId());
                ps.setString(8, ruleExS.getCustId());
                ps.setString(9, ruleExS.getRealFlowId());
            }
            @Override
            public int getBatchSize() {
                return ruleSList.size();
            }
        });

        return true;
    }

    @Override
    public boolean batchInsertRuleF(List<RuleExF> ruleFList, String qry) {
        jdbcTemplate.batchUpdate(qry, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                RuleExF ruleExF = ruleFList.get(i);
                ps.setBigDecimal(1, ruleExF.getRebmDetcId());
                ps.setString(2, ruleExF.getExCampId());
                ps.setString(3, ruleExF.getStepId());
                ps.setString(4, ruleExF.getDetcRouteId());
                ps.setString(5, ruleExF.getWorkDtmMil());
                ps.setString(6, ruleExF.getStopNodeId());
                ps.setString(7, ruleExF.getExTerm());
                ps.setString(8, ruleExF.getCampId());
                ps.setString(9, ruleExF.getCustId());
                ps.setString(10, ruleExF.getRealFlowId());
                ps.setString(11, ruleExF.getStopNodeItem());
            }
            @Override
            public int getBatchSize() {
                return ruleFList.size();
            }
        });

        return true;
    }

    @Override
    public boolean batchInsertClan(List<ClanEx> clanList, String qry) {
        jdbcTemplate.batchUpdate(qry, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ClanEx clanEx = clanList.get(i);
                ps.setBigDecimal(1, clanEx.getRebmDetcId());
                ps.setString(2, clanEx.getExCampId());
                ps.setString(3, clanEx.getActId());
                ps.setString(4, clanEx.getStatCd());
                ps.setString(5, clanEx.getWorkSvrNm());
                ps.setString(6, clanEx.getChanCd());
                ps.setString(7, clanEx.getCustId());
                ps.setString(8, clanEx.getOlppExdBrchCd());
                ps.setString(9, clanEx.getOlppExdKindCd());
                ps.setString(10, clanEx.getWorkDtmMil());
                ps.setString(11, clanEx.getClanDesc());
                ps.setString(12, clanEx.getStepId());
                ps.setString(13, clanEx.getDetcRouteId());
                ps.setString(14, clanEx.getCampId());
                ps.setString(15, clanEx.getExTerm());
                ps.setString(16, clanEx.getWorkDtm());
                ps.setString(17, clanEx.getRealFlowId());
            }
            @Override
            public int getBatchSize() {
                return clanList.size();
            }
        });

        return true;
    }

    @Override
    public boolean batchInsertChan(List<ChanEx> chanList, String qry) {
        jdbcTemplate.batchUpdate(qry, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ChanEx chanEx = chanList.get(i);
                ps.setBigDecimal(1, chanEx.getRebmDetcId());
                ps.setString(2, chanEx.getExCampId());
                ps.setString(3, chanEx.getActId());
                ps.setString(4, chanEx.getStatCd());
                ps.setString(5, chanEx.getWorkSvrNm());
                ps.setString(6, chanEx.getWorkSvrNm());
                ps.setString(7, chanEx.getWorkSvrNm());
                ps.setString(8, chanEx.getWorkSvrNm());
                ps.setString(9, chanEx.getWorkSvrNm());
                ps.setString(10, chanEx.getWorkSvrNm());
                ps.setString(11, chanEx.getWorkSvrNm());
                ps.setString(12, chanEx.getWorkSvrNm());
                ps.setString(13, chanEx.getWorkSvrNm());
                ps.setString(14, chanEx.getWorkSvrNm());
                ps.setString(15, chanEx.getWorkSvrNm());
                ps.setString(16, chanEx.getWorkSvrNm());
                ps.setString(17, chanEx.getWorkSvrNm());
                ps.setString(18, chanEx.getWorkSvrNm());
                ps.setString(19, chanEx.getWorkSvrNm());
                ps.setString(20, chanEx.getWorkSvrNm());
                ps.setString(21, chanEx.getWorkSvrNm());
                ps.setString(22, chanEx.getWorkSvrNm());
            }
            @Override
            public int getBatchSize() {
                return chanList.size();
            }
        });

        return true;
    }




//
//    private RowMapper<DetcMstr> detcMstrRowMapper() {
//        return (rs, rowNum) -> {
//            DetcMstr detcMstr = new DetcMstr();
//            detcMstr.setRebmDetcId(rs.getString("REBM_DETC_ID"));
//            detcMstr.setWorkSvrNm(rs.getString("WORK_SVR_NM"));
//            detcMstr.setWorkSvrId(rs.getString("WORK_SVR_ID"));
//            detcMstr.setDetcChanCd(rs.getString("DETC_CHAN_CD"));
//            detcMstr.setEvtOccrDtm(rs.getString("EVT_OCCUR_DTM"));
//            return detcMstr;
//        };
//    }

}
