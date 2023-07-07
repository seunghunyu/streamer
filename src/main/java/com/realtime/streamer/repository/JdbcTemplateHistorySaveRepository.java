package com.realtime.streamer.repository;

import com.realtime.streamer.data.DetcChanSqlInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.Base64;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Repository
public class JdbcTemplateHistorySaveRepository implements HistorySaveRepository{
    @Autowired
    private final JdbcTemplate jdbcTemplate;

    @Override
    public boolean saveDetc() {
        return false;
    }

    @Override
    public boolean saveRuleS() {
        return false;
    }

    @Override
    public boolean saveRuleF() {
        return false;
    }

    @Override
    public boolean saveClan() {
        return false;
    }


    private RowMapper<DetcChanSqlInfo> detChanSqlInfoRowMapper() {
        return (rs, rowNum) -> {
            DetcChanSqlInfo detcChanSqlInfo = new DetcChanSqlInfo();
            detcChanSqlInfo.setDetcChanCd(rs.getString("DETC_CHAN_CD"));
            detcChanSqlInfo.setSqlKind(rs.getString("SQL_KIND"));
            detcChanSqlInfo.setSqlType(rs.getString("SQL_TYPE"));
            detcChanSqlInfo.setSqlStep(rs.getString("SQL_STEP"));
            detcChanSqlInfo.setSqlScrt(rs.getString("SQL_SCRT"));
            detcChanSqlInfo.setEncodeyn(rs.getString("ENCODE_YN"));
            return detcChanSqlInfo;
        };
    }


}
