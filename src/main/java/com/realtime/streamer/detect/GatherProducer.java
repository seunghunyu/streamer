package com.realtime.streamer.detect;

import lombok.RequiredArgsConstructor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
/*
*[2023.06.27] 신규 생성
*
*
 */
@RequiredArgsConstructor
public class GatherProducer {
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    String IP = "";
    String GroupId = "";
    String Topic = "";


}
