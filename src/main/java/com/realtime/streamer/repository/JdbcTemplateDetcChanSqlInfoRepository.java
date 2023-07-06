package com.realtime.streamer.repository;

import com.realtime.streamer.data.DetcChan;
import com.realtime.streamer.data.DetcChanSqlInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Repository
public class JdbcTemplateDetcChanSqlInfoRepository implements DetcChanSqlInfoReposiotry{
    @Autowired
    private final JdbcTemplate jdbcTemplate;


    @Override
    public List<DetcChanSqlInfo> getUseDetcChanSqlList() {
        return jdbcTemplate.query(" SELECT DETC_CHAN_CD, SQL_KIND, SQL_TYPE, SQL_STEP, SQL_SCRT, ENCODE_YN  " +
                                         " FROM R_REBM_DETC_CHAN_SQL WHERE SQL_KIND = '1' AND SQL_TYPE = '3' ", detChanSqlInfoRowMapper());
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
