package com.realtime.streamer.repository.rebm;

import com.realtime.streamer.data.PsnlTag;
import com.realtime.streamer.data.Scrt;
import com.realtime.streamer.mappers.h2.H2PsnlTagMapper;
import com.realtime.streamer.mappers.rebm.PsnlTagMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
@RequiredArgsConstructor
public class MyBatisPsnlTagRepository implements PsnlTagRepository{

    private final PsnlTagMapper psnlTagMapper;
    private final H2PsnlTagMapper h2PsnlTagMapper;

    @Autowired
    @Qualifier("h2JdbcTemplate")
    private final JdbcTemplate h2JdbcTemplate;

    @Autowired
    @Qualifier("crmJdbcTemplate")
    private final JdbcTemplate crmJdbcTemplate;


    @Autowired
    private final JdbcTemplate rebmJdbcTemplate;

    @Override
    public List<PsnlTag> getAllPsnlTagList() {
        return psnlTagMapper.getAllPsnlTagList();
    }
    @Override
    public List<Scrt> getPsnlScrtInfo(String actId) {
        return psnlTagMapper.getPsnlScrtInfo(actId);
    }

    @Override
    public List<Map<String, Object>> getPsnlScrtColInfo(String dbPool, String qry, Object[] objArr) {
        List<Map<String, Object>> maps = rebmJdbcTemplate.queryForList(qry, objArr);
        return maps;
    }

    @Override
    public List<Scrt> getH2PsnlScrtInfo(String actId) {
        return h2PsnlTagMapper.getPsnlScrtInfo(actId);
    }


}
