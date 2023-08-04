package com.realtime.streamer.repository.rebm;

import com.realtime.streamer.data.DetcChan;
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
public class JdbcTemplateDetcChanRepository implements DetcChanReposiotry{
    @Autowired
    private final JdbcTemplate jdbcTemplate;

    @Override
    public List<DetcChan> getUseDetcChanList() {
        return jdbcTemplate.query(" SELECT DETC_CHAN_CD, DETC_CHAN_NM, STOP_REG_YN, USE_YN  FROM R_REBM_DETC_CHAN WHERE USE_YN = '1' ", detChanRowMapper());
    }

    private RowMapper<DetcChan> detChanRowMapper() {
        return (rs, rowNum) -> {
            DetcChan detcChan = new DetcChan();
            detcChan.setDetcChanCd(rs.getString("DETC_CHAN_CD"));
            detcChan.setDetcChanNm(rs.getString("DETC_CHAN_NM"));
            detcChan.setStopRegYn(rs.getString("STOP_REG_YN"));
            detcChan.setUseYn(rs.getString("USE_YN"));
            return detcChan;
        };
    }

}
