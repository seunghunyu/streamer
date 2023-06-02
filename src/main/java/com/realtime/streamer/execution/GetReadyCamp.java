package com.realtime.streamer.execution;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

@Slf4j
public class GetReadyCamp {

    String getDetcChanQry = " SELECT B.DETC_CHAN_CD, COUNT(*) "
                          + "   FROM ECUBEEBM.R_PLAN A, ECUBEEBM.R_DETC_CHAN B "
                          + "  WHERE A.CAMP_ID = B.CAMP_ID "
                          + "    AND A.CAMP_STAT in ('3100', '3000') "
                          + "    AND A.CAMP_EX_TY = '9' "
                          + "    AND A.CAMP_STR_DT <= ? "
                          + "    AND A.CAMP_END_DT >= ? ";


    public void polling(){
        System.out.println("polling :: " );
        log.debug("GetReadyCamp polling Interval : " );



    }

}
