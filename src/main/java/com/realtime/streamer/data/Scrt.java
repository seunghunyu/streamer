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
}
