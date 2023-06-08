package com.realtime.streamer.repository;

import com.realtime.streamer.data.Camp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
@Slf4j
@Transactional
@SpringBootTest
class JdbcTemplateRepositoryTest {

    @Autowired
    JdbcTemplateRepository repository;
    DataSource dataSource;

    @Test
    void testConnecion(){
        System.out.println("test Connection ");
        log.info("qqqqqqqqqqqq");
        log.debug("2222222222");
        log.trace("qqqqqqqweweqweqwe");
//        Connection con = null;
//        try{
//            con = dataSource.getConnection();
//        }catch (Exception e){
//
//        }finally{
//
//        }
    }

    @Test
    void getCampList() {
        List<Camp> camp =  repository.getCampList();
        System.out.println("R_PLAN Count :::  "+camp.size());
        //log.info("test success");
    }

    @Test
    void getCampOne() {
    }

    @Test
    void findById() {

    }

    @Test
    void findByDt() {
    }

}