package com.realtime.streamer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.*;
@SpringBootTest
@Transactional
class StreamerApplicationTest {
    @Autowired
    //@Qualifier(value = "metaDataSource")
    private DataSource dataSource;

    @Test
    public void connectionTest(){
        try{
            System.out.println("con start");
            Connection con = dataSource.getConnection();
            System.out.println("con end");
        }catch(Exception e){
            System.out.println("Connection failed");
            e.printStackTrace();
        }

    }

}