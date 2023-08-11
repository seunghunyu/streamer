package com.realtime.streamer.data;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class Scrt {
    String actId;
    String campId;
    String actNm;
    String actTy;
    String chanCd;
    String actUntAmt;
    String stepId;
    String detcRouteId;
    String contSetYn;
    String scrtId;

    String sqlScrt1;
    String sqlScrt2;
    String sqlScrt3;
    String psnlTagNmGrp;
    String psnlSqlColmgrp;
    String dbPool;
    String rplcVarNmGrp;
    String rplcVarTyGrp;
    String ptagSqlId;

}
