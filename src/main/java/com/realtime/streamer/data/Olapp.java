package com.realtime.streamer.data;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/*
*
*  �ߺ� ���� ���� ������
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
