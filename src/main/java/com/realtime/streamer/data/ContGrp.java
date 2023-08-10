package com.realtime.streamer.data;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * 대조군 관련
 */
@Data
@RequiredArgsConstructor
public class ContGrp {
    String contSetObjId;
    String realFlowId;
    String campId;
    Integer contSetRatio;
    Integer contSetMaxCnt;
    String exUnitCd;
    String aplyObjId;
    String contSetCratCnt;
    String clsngYn;

    String custId;
    String cratDtm;
    String contSetYn;
    String actId;
    String workDtmMi;
    String cratDt;
    String statCd;
}
