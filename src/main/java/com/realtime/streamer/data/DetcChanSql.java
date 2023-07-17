package com.realtime.streamer.data;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class DetcChanSql {
    String detcChanCd;
    String sqlKind;
    String sqlType;
    String sqlStep;
    String sqlScrt;
    String encodeyn;
}
