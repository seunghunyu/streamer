package com.realtime.streamer.data;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class DetcChanSqlInfo {
    String detcChanCd;
    String sqlKind;
    String dbPool;
    String selItem;
    String selType;
}
