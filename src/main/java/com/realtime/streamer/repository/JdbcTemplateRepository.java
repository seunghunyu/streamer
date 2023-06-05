package com.realtime.streamer.repository;

import com.realtime.streamer.data.Camp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Repository
public class JdbcTemplateRepository implements CampRepository{
    private final JdbcTemplate jdbcTemplate;

    public JdbcTemplateRepository(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }


    @Override
    public List<Camp> getCampList() {
        return null;
    }

    @Override
    public Camp getCampOne() {
        return null;
    }
}
