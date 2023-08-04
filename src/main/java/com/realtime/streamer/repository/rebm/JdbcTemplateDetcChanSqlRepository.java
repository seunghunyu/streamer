package com.realtime.streamer.repository.rebm;

import com.realtime.streamer.data.DetcChanSql;
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
public class JdbcTemplateDetcChanSqlRepository implements DetcChanSqlInfoReposiotry{
    @Autowired
    private final JdbcTemplate jdbcTemplate;


    @Override
    public List<DetcChanSql> getUseDetcChanSqlList() {
        return jdbcTemplate.query(" SELECT DETC_CHAN_CD, SQL_KIND, SQL_TYPE, SQL_STEP, SQL_SCRT, ENCODE_YN  " +
                                         " FROM R_REBM_DETC_CHAN_SQL WHERE SQL_KIND = '2' AND SQL_TYPE = '1' ", detChanSqlRowMapper());
    }

    @Override
    public String findByOne(String detcChanCd) {
        DetcChanSql DetcChanSqlInfo = jdbcTemplate.queryForObject(
                "SELECT SQL_SCRT FROM R_REBM_DETC_CHAN_SQL WHERE SQL_KIND = '2' AND SQL_TYPE = '1' AND DETC_CHAN_CD = ? " ,
                (resultSet, rowNum) -> {
                    DetcChanSql sqlInfo = new DetcChanSql();
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

    private RowMapper<DetcChanSql> detChanSqlRowMapper() {
        return (rs, rowNum) -> {
            DetcChanSql detcChanSql = new DetcChanSql();
            detcChanSql.setDetcChanCd(rs.getString("DETC_CHAN_CD"));
            detcChanSql.setSqlKind(rs.getString("SQL_KIND"));
            detcChanSql.setSqlType(rs.getString("SQL_TYPE"));
            detcChanSql.setSqlStep(rs.getString("SQL_STEP"));
            detcChanSql.setSqlScrt(rs.getString("SQL_SCRT"));
            detcChanSql.setEncodeyn(rs.getString("ENCODE_YN"));
            return detcChanSql;
        };
    }


}
