package com.realtime.streamer.data;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;

@Data
@RequiredArgsConstructor
public class ClanEx {
    BigDecimal rebmDetcId;
    String exCampId;
    String actId;
    String statCd;
    String workSvrNm;
    String chanCd;
    String custId;
    String olppExdBrchCd;
    String olppExdKindCd;
    String workDtmMil;
    String clanDesc;
    String stepId;
    String detcRouteId;
    String campId;
    String exTerm;
    String workDtm;
    String realFlowId;
}
