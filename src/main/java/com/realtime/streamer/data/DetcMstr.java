package com.realtime.streamer.data;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;

@Data
@RequiredArgsConstructor
public class DetcMstr {
    BigDecimal rebmDetcId;
    String workSvrNm;
    String workSvrId;
    String detcChanCd;
    String evtOccrDtm;
    String custId;
    String cratMethCd;
    Integer workDtmMil;
    String statCd;
    String stopNodeId;
    String COL01;
    String COL02;
    String COL03;
    String COL04;
    String COL05;
    String COL06;
    String COL07;
    String COL08;
    String COL09;
    String COL10;
}
