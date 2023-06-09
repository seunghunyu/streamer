package com.realtime.streamer.execution;

import com.realtime.streamer.repository.JdbcTemplateCampRepository;
import lombok.extern.slf4j.Slf4j;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

@Slf4j
//@Component
public class GetReadyCamp {

    //@Autowired
    private JdbcTemplateCampRepository repository;

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
