package com.realtime.streamer.data;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class PsnlTag {
    String actId;
    String psnlTagNm;
    String campId;
    String psnlTagKind;
}
