package com.realtime.streamer.producer;

import lombok.RequiredArgsConstructor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

@RequiredArgsConstructor
public class KafkaProducer1 {
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    String IP = "";
    String GroupId = "";
    String Topic = "";


}
