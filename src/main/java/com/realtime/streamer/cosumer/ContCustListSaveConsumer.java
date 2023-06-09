package com.realtime.streamer.cosumer;

public class ContCustListSaveConsumer {
    String instSql = " INSERT INTO R_REBM_CONT_CUST_LIST (CUST_ID, CONT_SET_OBJ_ID, CRAT_DTM, CONT_SET_YN, ACT_ID,    WORK_DTM_MIL, CAMP_ID, CRAT_DT) "
                   + " VALUES (?,?,?,?,?,   ?,?,?) ";
}
