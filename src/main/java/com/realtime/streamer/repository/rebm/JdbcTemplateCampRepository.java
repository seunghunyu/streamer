package com.realtime.streamer.repository.rebm;

import com.realtime.streamer.data.Camp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.text.SimpleDateFormat;
import java.util.Calendar;
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
            Rowmapper 사용시 RowMapper에 선언된 모든 컬럼을 다 입력해야 에러가 안남
            return jdbcTemplate.query(" SELECT CAMP_ID FROM R_PLAN ", campRowMapper()); -> 이렇게 사용시 아래와 같은 에러 발생
            org.springframework.jdbc.BadSqlGrammarException: StatementCallback; bad SQL grammar [ SELECT CAMP_ID FROM R_PLAN ]; nested exception is java.sql.SQLException: 부적합한 열 이름
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
    //사용중인 캠페인 조회
    @Override
    public List<Camp> getDetcChanList() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar c1 = Calendar.getInstance();

        String strDt = sdf.format(c1.getTime());
        String endDt = sdf.format(c1.getTime());
        System.out.println("TODAY :::" + strDt);

        List<Camp> result = jdbcTemplate.query(" SELECT B.DETC_CHAN_CD, COUNT(*) AS CNT"
                                                + " FROM R_PLAN A, R_FLOW_DETC_CHAN B "
                                                + " WHERE A.CAMP_ID = B.CAMP_ID "
                                                + "   AND A.CAMP_STAT in ('3100', '3000') "
                                                + "   AND A.CAMP_EX_TY = '9' "
                                                + "   AND A.CAMP_STR_DT <= ? "
                                                + "   AND A.CAMP_END_DT >= ? "
                                                + " GROUP BY B.DETC_CHAN_CD ",
                (resultSet, rowNum) -> {
                    Camp newCamp = new Camp();
                    newCamp.setDetcChanCd(resultSet.getString("DETC_CHAN_CD"));
                    newCamp.setCount(resultSet.getInt("CNT"));
                    return newCamp;
                }, strDt, endDt);

        return result;
    }

    @Override
    public List<Camp> getCampBrch() {
        return null;
    }

    @Override
    public List<Camp> getFlowStat(String endDt) {
        return null;
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
