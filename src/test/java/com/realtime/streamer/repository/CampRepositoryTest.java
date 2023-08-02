package com.realtime.streamer.repository;

import com.realtime.streamer.data.Camp;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class CampRepositoryTest {

    @Autowired
    MyBatisCampRepository repository;

    @Test
    void getCamp(){

        Camp camp = repository.getCampOne("C221128903");
        System.out.println(camp);

    }
}
