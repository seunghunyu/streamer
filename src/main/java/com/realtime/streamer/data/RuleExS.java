package com.realtime.streamer.data;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;

@Data
@RequiredArgsConstructor
public class RuleExS {
    BigDecimal rebmDetcId;
    String exCampId;
    String stepId;
    String detcRouteId;
    String workDtmMil;
    String exTerm;
    String campId;
    String custId;
    String realFlowId;
}
