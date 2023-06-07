package com.realtime.streamer.repository;

import com.realtime.streamer.data.Camp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

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
        return jdbcTemplate.query("select * from r_plan", campRowMapper());
    }

    @Override
    public Camp getCampOne() {
       // String name = jdbcTemplate.queryForObject("SELECT name FROM USER WHERE id=?", String.class,1000L);

        Camp camp = jdbcTemplate.queryForObject(
                "select camp_id, camp_nm, camp_stat from r_plan where camp_id = ?",
                (resultSet, rowNum) -> {
                    Camp newCamp = new Camp();
                    newCamp.setCampId(resultSet.getString("camp_id"));
                    newCamp.setCampName(resultSet.getString("camp_nm"));
                    newCamp.setCampStat(resultSet.getString("camp_stat"));
                    return newCamp;
                },
                1212L);
        return camp;
    }

    @Override
    public Optional<Camp> findById(String id) {
        List<Camp> result = jdbcTemplate.query(" select * from r_plan where camp_id = ? ", campRowMapper(), id);
        return result.stream().findAny();
        //return Optional.empty();
    }

    @Override
    public Optional<Camp> findByDt(String strDt, String endDt) {
        List<Camp> result = jdbcTemplate.query(" select * from r_plan where camp_str_dt >= ? and camp_end_dt <= ? ", campRowMapper(), strDt, endDt);
        return result.stream().findAny();
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
