package com.realtime.streamer.util;

import jdk.jshell.execution.Util;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;


/*
*  [2023.07.17] 데이터 load 작업  클래스
* */
@Order(1)
@EnableAsync
@Component
public class DataLoader implements CommandLineRunner {
    @Autowired
    Utility utility;

    @Override
    public void run(String... args) throws Exception {
        System.out.println("REDIS DATA LOADER ::::::::::::::::::::::::::::::::::::START");
        //1. 사용중인 감지채널 Redis 서버 업로드
        utility.setRedisDetcChanList();
        //2. 사용중인 감지테이블 저장 쿼리 Redis 서버 업로드
        utility.setRedisDetcChanInstSqlList();
        //3. 사용중인 감지채널 사용 아이템 Redis 서버 업로드
        utility.setRedisDetcChanAddInfoItem();

        System.out.println("REDIS DATA LOADER ::::::::::::::::::::::::::::::::::::END");

        //System.out.println("REDIS DATA LOADER ::::::::::::::::::::::::::::::::::::" + utility.getRedisDetcChanInstSqlList("9001"));

    }
}
