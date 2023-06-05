package com.realtime.streamer.data;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class Camp {
    private final String campId;
    private final String campName;
    private final String campStat;
    private final String campStrDt;
    private final String campEndDt;
}
