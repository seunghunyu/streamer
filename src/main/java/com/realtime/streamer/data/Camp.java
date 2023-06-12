package com.realtime.streamer.data;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class Camp {
    String campId;
    String campName;
    String campStat;
    String campStrDt;
    String campEndDt;
    String detcChanCd;
    int count;


    public String getCampId() {
        return campId;
    }

    public void setCampId(String campId) {
        this.campId = campId;
    }

    public String getCampName() {
        return campName;
    }

    public void setCampName(String campName) {
        this.campName = campName;
    }

    public String getCampStat() {
        return campStat;
    }

    public void setCampStat(String campStat) {
        this.campStat = campStat;
    }

    public String getCampStrDt() {
        return campStrDt;
    }

    public void setCampStrDt(String campStrDt) {
        this.campStrDt = campStrDt;
    }

    public String getCampEndDt() {
        return campEndDt;
    }

    public void setCampEndDt(String campEndDt) {
        this.campEndDt = campEndDt;
    }
}
