package com.realtime.streamer.repository;

import com.realtime.streamer.data.Camp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import javax.sql.RowSet;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@Repository
public class JdbcTemplateCampRepository implements CampRepository{
    @Autowired
    private final JdbcTemplate jdbcTemplate;

    @Override
    public List<Camp> getCampList() {
        return jdbcTemplate.query(" SELECT CAMP_ID, CAMP_NM, CAMP_STAT, CAMP_STR_DT, CAMP_END_DT  FROM R_PLAN ", campRowMapper());
        /*
            Rowmapper ���� RowMapper�� ����� ��� �÷��� �� �Է��ؾ� ������ �ȳ�
            return jdbcTemplate.query(" SELECT CAMP_ID FROM R_PLAN ", campRowMapper()); -> �̷��� ���� �Ʒ��� ���� ���� �߻�
            org.springframework.jdbc.BadSqlGrammarException: StatementCallback; bad SQL grammar [ SELECT CAMP_ID FROM R_PLAN ]; nested exception is java.sql.SQLException: �������� �� �̸�
         */
    }

    @Override
    public Camp getCampOne(String id) {
        Camp camp = jdbcTemplate.queryForObject(
                "SELECT CAMP_ID, CAMP_NM, CAMP_STAT FROM R_PLAN WHERE CAMP_ID = ? ",
                (resultSet, rowNum) -> {
                    Camp newCamp = new Camp();
                    newCamp.setCampId(resultSet.getString("camp_id"));
                    newCamp.setCampName(resultSet.getString("camp_nm"));
                    newCamp.setCampStat(resultSet.getString("camp_stat"));
                    return newCamp;
                }, id);
        return camp;
    }

    @Override
    public Optional<Camp> findById(String id) {
        List<Camp> result = jdbcTemplate.query(" SELECT CAMP_ID, CAMP_NM, CAMP_STAT, CAMP_STR_DT, CAMP_END_DT " +
                "FROM R_PLAN WHERE CAMP_ID = ? ", campRowMapper(), id);
        return result.stream().findAny();
        //return Optional.empty();
    }

    @Override
    public Optional<Camp> findByDt(String strDt, String endDt) {
        List<Camp> result = jdbcTemplate.query(" SELECT CAMP_ID, CAMP_NM, CAMP_STAT, CAMP_STR_DT, CAMP_END_DT " +
                "FROM R_PLAN WHERE CAMP_STR_DT >= ? AND CAMP_END_DT <= ? ", campRowMapper(), strDt, endDt);
        return result.stream().findAny();
    }

    @Override
    public int getCampCount() {
        int count = jdbcTemplate.queryForObject(" SELECT COUNT(*) FROM R_PLAN ", Integer.class);
        return count;
    }

    private RowMapper<Camp> campRowMapper() {
        return (rs, rowNum) -> {
            Camp camp = new Camp();
            camp.setCampId(rs.getString("CAMP_ID"));
            camp.setCampName(rs.getString("CAMP_NM"));
            camp.setCampStat(rs.getString("CAMP_STAT"));
            camp.setCampStrDt(rs.getString("CAMP_STR_DT"));
            camp.setCampEndDt(rs.getString("CAMP_END_DT"));
            return camp;
        };
    }

}
