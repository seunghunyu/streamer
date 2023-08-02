package com.realtime.streamer.data;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/*
*
*  중복 제거 관련 데이터
*
* */
@Data
@RequiredArgsConstructor
public class Olapp {
    String actId;
    String olappKindCd;
    String olappUseObjId;
    String olappObjId;
    String campId;
    String realFlowId;

    String excldCondId;
}
