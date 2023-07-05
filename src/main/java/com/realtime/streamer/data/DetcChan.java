package com.realtime.streamer.data;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class DetcChan {
    String detcChanCd;
    String detcChanNm;
    String stopRegYn;
    String useYn;
}
