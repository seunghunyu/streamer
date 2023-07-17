package com.realtime.streamer.util;

import jdk.jshell.execution.Util;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;


/*
*  [2023.07.17] ������ load �۾�  Ŭ����
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
        //1. ������� ����ä�� Redis ���� ���ε�
        utility.setRedisDetcChanList();
        //2. ������� �������̺� ���� ���� Redis ���� ���ε�
        utility.setRedisDetcChanInstSqlList();
        //3. ������� ����ä�� ��� ������ Redis ���� ���ε�
        utility.setRedisDetcChanAddInfoItem();

        System.out.println("REDIS DATA LOADER ::::::::::::::::::::::::::::::::::::END");

        //System.out.println("REDIS DATA LOADER ::::::::::::::::::::::::::::::::::::" + utility.getRedisDetcChanInstSqlList("9001"));

    }
}
