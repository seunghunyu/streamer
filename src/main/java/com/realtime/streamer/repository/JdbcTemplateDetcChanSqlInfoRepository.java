package com.realtime.streamer.repository;

import com.realtime.streamer.data.Camp;
import com.realtime.streamer.data.DetcChan;
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
public class JdbcTemplateDetcChanSqlInfoRepository implements DetcChanSqlInfoReposiotry{
    @Autowired
    private final JdbcTemplate jdbcTemplate;


    @Override
    public List<DetcChanSqlInfo> getUseDetcChanSqlList() {
        return jdbcTemplate.query(" SELECT DETC_CHAN_CD, SQL_KIND, SQL_TYPE, SQL_STEP, SQL_SCRT, ENCODE_YN  " +
                                         " FROM R_REBM_DETC_CHAN_SQL WHERE SQL_KIND = '2' AND SQL_TYPE = '1' ", detChanSqlInfoRowMapper());
    }

    @Override
    public String findByOne(String detcChanCd) {
        DetcChanSqlInfo DetcChanSqlInfo = jdbcTemplate.queryForObject(
                "SELECT SQL_SCRT FROM R_REBM_DETC_CHAN_SQL WHERE SQL_KIND = '2' AND SQL_TYPE = '1' AND DETC_CHAN_CD = ? " ,
                (resultSet, rowNum) -> {
                    DetcChanSqlInfo sqlInfo = new DetcChanSqlInfo();
                    sqlInfo.setSqlScrt(resultSet.getString("SQL_SCRT"));
                    return sqlInfo;
                }, detcChanCd);

        byte[] decodedBytes =  null;
        String decodedString = "";

        if(DetcChanSqlInfo.getSqlScrt().length() > 3) {
            decodedBytes = Base64.getDecoder().decode(DetcChanSqlInfo.getSqlScrt().substring(3));
        }else{
            decodedBytes = Base64.getDecoder().decode(DetcChanSqlInfo.getSqlScrt());
        }

        return new String(decodedBytes);
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
