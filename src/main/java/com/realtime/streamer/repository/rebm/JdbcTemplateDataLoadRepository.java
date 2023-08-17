package com.realtime.streamer.repository.rebm;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Slf4j
@RequiredArgsConstructor
@Repository
public class JdbcTemplateDataLoadRepository implements DataLoadRepository {
    @Autowired
    @Qualifier("h2JdbcTemplate")
    private final JdbcTemplate jdbcTemplate;

    @Override
    public void dataLoad() {
        String select_cust_id_from_member = jdbcTemplate.queryForObject("SELECT NAME FROM MEMBER", String.class);
        log.info("select_cust_id_from_member :::::::" + select_cust_id_from_member);
        System.out.println("select_cust_id_from_member:::::::" + select_cust_id_from_member);
    }
}
