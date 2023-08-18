package com.realtime.streamer.repository.rebm;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@Repository
public class JdbcTemplateDataLoadRepository implements DataLoadRepository {
    @Autowired
    @Qualifier("h2JdbcTemplate")
    private final JdbcTemplate h2JdbcTemplate;

    @Autowired
    @Qualifier("crmJdbcTemplate")
    private final JdbcTemplate crmJdbcTemplate;


    @Autowired
    private final JdbcTemplate rebmJdbcTemplate;


    @Override
    public void dataLoad() {
        String select_cust_id_from_member = h2JdbcTemplate.queryForObject("SELECT NAME FROM MEMBER", String.class);
        log.info("select_cust_id_from_member :::::::" + select_cust_id_from_member);
        System.out.println("select_cust_id_from_member:::::::" + select_cust_id_from_member);
    }

    @Override
    public void createTable(String qry) {
        h2JdbcTemplate.execute(qry);
    }

    @Override
    public void createIndex(String qry) {
        h2JdbcTemplate.execute(qry);
    }

    @Override
    public void insertData(String qry, Map<String, Object> dataMap) {
        Iterator<String> itr = dataMap.keySet().iterator();
        ArrayList<Object> paramList = new ArrayList<>();
        //SqlParameterSource parameterSource = new MapSqlParameterSource("map",dataMap);
        while (itr.hasNext()){
            String key = itr.next();
            log.info("key = {}, valueClass = {}", key, dataMap.get(key));
            paramList.add(dataMap.get(key));
        }
        h2JdbcTemplate.update(qry, paramList);
        //h2JdbcTemplate.execute(qry);
    }

    @Override
    public List<?> selectData(String qry, String dbPool) {
        System.out.println("DB POOL:::::::::::::::::::::::" + dbPool);
        List<Map<String, Object>> maps = null;
        if(dbPool.equals("REBMDB")) {
            maps = rebmJdbcTemplate.queryForList(qry);
        }else if(dbPool.equals("CRMDB")){
            maps = crmJdbcTemplate.queryForList(qry);
        }else{
            maps = h2JdbcTemplate.queryForList(qry);
        }

        return maps;
    }
}
