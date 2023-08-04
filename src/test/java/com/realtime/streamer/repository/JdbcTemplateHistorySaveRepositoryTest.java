package com.realtime.streamer.repository;

import com.realtime.streamer.data.DetcMstr;
import com.realtime.streamer.repository.rebm.JdbcTemplateHistorySaveRepository;
import com.realtime.streamer.util.Utility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest
class JdbcTemplateHistorySaveRepositoryTest {

    @Autowired
    Utility utility;

    @Autowired
    JdbcTemplateHistorySaveRepository repository;

//    @Autowired
//    DetcMstr detcMstr;

    @Test
    void batchInsertDetc() {
        String tabledt = utility.getTableDtNum();
        List<DetcMstr> detcMstrList  = new ArrayList<>();
        BigDecimal number = new BigDecimal(1);
        for(int i = 1 ; i < 11 ; i++){
            DetcMstr detcMstr = new DetcMstr();
            detcMstr.setRebmDetcId(new BigDecimal(i));
            detcMstr.setWorkSvrNm("A"+Integer.toString(i));
            detcMstr.setWorkSvrId("S");
            detcMstr.setDetcChanCd("9001");
            detcMstr.setCratMethCd("T");
            detcMstr.setWorkDtmMil(1);
            detcMstr.setStatCd("S");
            detcMstr.setStopNodeId("");
            detcMstr.setCustId("C1121212121");
            detcMstr.setEvtOccrDtm("test");
            detcMstr.setCOL01("");
            detcMstr.setCOL02("");
            detcMstr.setCOL03("");
            detcMstr.setCOL04("");
            detcMstr.setCOL05("");
            detcMstr.setCOL06("");
            detcMstr.setCOL07("");
            detcMstr.setCOL08("");
            detcMstr.setCOL09("");
            detcMstr.setCOL10("");

            System.out.println(detcMstr);
            detcMstrList.add(detcMstr);
        }

        String qry = " INSERT INTO R_REBM_DETC_MSTR_1 ( "
                   + "     REBM_DETECT_ID, WORK_SVR_NM, WORK_SVR_ID, DETC_CHAN_CD, CRAT_METH_CD,    "
                    + "     WORK_DTM_MIL, STAT_CD, STOP_NODE_ID, CUST_ID, EVT_OCCR_DTM, "
                    + "     COL01, COL02, COL03, COL04, COL05, COL06, COL07, COL08, COL09, COL10)   "
                    + "     VALUES( "
                    + "             ?, ?, ?, ?, ?,  "
                    + "            ?, ?, ?, ?, ?,   "
                    + "           ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
        qry = qry.replace( "R_REBM_DETC_MSTR_1", "R_REBM_DETC_MSTR_"+tabledt);

        boolean flag = repository.batchInsertDetc(detcMstrList, qry);

        System.out.println("detcMstrQry SUCCESS YN @@@@"+flag);

    }

    @Test
    void batchInsertRuleS() {
    }

    @Test
    void batchInsertRuleF() {
    }

    @Test
    void batchInsertClan() {
    }

    @Test
    void batchInsertChan() {
    }
}