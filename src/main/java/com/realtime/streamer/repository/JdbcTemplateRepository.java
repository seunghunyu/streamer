package com.realtime.streamer.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;

@Slf4j
@RequiredArgsConstructor
//@Repository
public class JdbcTemplateRepository {
    private final JdbcTemplate jdbcTemplate;

    public JdbcTemplateRepository(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public int getCount(String countQuery){
        System.out.println("getCount");
        return jdbcTemplate.queryForObject(countQuery, Integer.class);
    }


}
