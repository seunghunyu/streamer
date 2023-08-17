package com.realtime.streamer.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@Transactional
@SpringBootTest
class DataLoadServiceTest {
    @Autowired
    DataLoadService dataLoadService;

    @Test
    void getMemberName(){
        dataLoadService.dataLoad();
    }
}