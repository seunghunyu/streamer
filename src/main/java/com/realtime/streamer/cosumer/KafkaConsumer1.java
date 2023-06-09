package com.realtime.streamer.cosumer;

import lombok.RequiredArgsConstructor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class KafkaConsumer1 {
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    String IP = "";
    String GroupId = "";
    String Topic = "";

}
