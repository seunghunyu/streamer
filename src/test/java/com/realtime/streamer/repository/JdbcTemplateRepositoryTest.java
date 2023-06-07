package com.realtime.streamer.repository;

import com.realtime.streamer.data.Camp;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.util.List;

@Transactional
@SpringBootTest
class JdbcTemplateRepositoryTest {

    private final JdbcTemplate jdbcTemplate;
    @Autowired
    JdbcTemplateRepository repository;

    @Autowired
    public JdbcTemplateRepositoryTest(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Test
    void getCampList() {
        List<Camp> camp =  repository.getCampList();
        System.out.println(camp);
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
    private RowMapper<Camp> campRowMapper() {
        return (rs, rowNum) -> {
            Camp camp = new Camp();
            camp.setCampId(rs.getString("camp_id"));
            camp.setCampName(rs.getString("camp_name"));
            camp.setCampStat(rs.getString("camp_stat"));
            camp.setCampStrDt(rs.getString("camp_str_dt"));
            camp.setCampEndDt(rs.getString("camp_end_dt"));
            return camp;
        };
    }
}