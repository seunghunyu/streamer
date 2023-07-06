package com.realtime.streamer.service;

import com.realtime.streamer.data.Camp;
import com.realtime.streamer.repository.JdbcTemplateCampRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
@Slf4j
@Transactional
@SpringBootTest
class CampServiceTest {
    @Autowired
    JdbcTemplateCampRepository repository;
    @Test
    void findById() {
        Optional<Camp> camp = repository.findById("C22052390M");
        System.out.println(camp);
    }
}