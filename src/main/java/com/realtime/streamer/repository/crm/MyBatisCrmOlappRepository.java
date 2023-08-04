package com.realtime.streamer.repository.crm;

import com.realtime.streamer.data.Olapp;
import com.realtime.streamer.mappers.crm.CrmOlappMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Repository
public class MyBatisCrmOlappRepository implements CrmOlappRepository {

    private final CrmOlappMapper olappMapper;

    @Override
    public List<Olapp> findFatChanInfo() {
        List<Olapp> fatChanInfo = olappMapper.findFatChanInfo();
        return fatChanInfo;
    }

    @Override
    public Integer findFatStupDay() {
        Integer fatStupDay = olappMapper.findFatStupDay();
        return fatStupDay;
    }

    @Override
    public Integer findFatStupCount() {
        Integer fatStupCount = olappMapper.findFatStupCount();
        return fatStupCount;
    }

}
