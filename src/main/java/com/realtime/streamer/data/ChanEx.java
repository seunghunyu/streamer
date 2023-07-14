package com.realtime.streamer.data;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;

@Data
@RequiredArgsConstructor
public class ChanEx {
    BigDecimal rebmDetcId;
    String exCampId;
    String actId;
    String statCd;
    String workSvrNm;
    String workSvrId;
    String chanCd;
    String custId;
    String scrtId;
    String rebmSendId;
    String detcChanCd;
    String contSetYn;
    String workDtmMil;
    String campId;
    String exActId;
    String stepId;
    String detcRouteId;
    String abtObjKind;
    String abtObjId;
    String exTerm;
    String workDtm;
    String realFlowId;
}
