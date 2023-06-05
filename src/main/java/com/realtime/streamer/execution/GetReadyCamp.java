package com.realtime.streamer.execution;

import com.realtime.streamer.repository.JdbcTemplateRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

@Slf4j
public class GetReadyCamp {

    @Autowired
    private JdbcTemplateRepository repository;

    String getDetcChanQry = " SELECT B.DETC_CHAN_CD, COUNT(*) "
                          + "   FROM ECUBEEBM.R_PLAN A, ECUBEEBM.R_FLOW_DETC_CHAN B "
                          + "  WHERE A.CAMP_ID = B.CAMP_ID "
                          + "    AND A.CAMP_STAT in ('3100', '3000') "
                          + "    AND A.CAMP_EX_TY = '9' "
                          + "    AND A.CAMP_STR_DT <= ? "
                          + "    AND A.CAMP_END_DT >= ? ";
    PreparedStatement pstmt = null;
    ResultSet rs = null;

    public void polling(){
        System.out.println("polling :: " );
        log.debug("GetReadyCamp polling Interval : " );

        //감치채널 갯수 있는지 체크
        //int count = repository.getCount(getDetcChanQry);
        //System.out.println("오늘 일자의 수행, 테스트수행에서 사용중인 감지채널 갯수 ::::::"+Integer.toString(count));

    }

}
