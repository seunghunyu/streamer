package com.realtime.streamer.execution;

import com.realtime.streamer.data.Camp;
import com.realtime.streamer.repository.JdbcTemplateRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Optional;

@Slf4j
//@Component
public class GetReadyCamp {

    //@Autowired
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
        System.out.println("####");
        //repository.getCampList();
        System.out.println("*****************");



    }

}
