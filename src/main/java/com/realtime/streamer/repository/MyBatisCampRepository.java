package com.realtime.streamer.repository;

import com.realtime.streamer.data.Camp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
@Slf4j
@RequiredArgsConstructor
@Repository
public class MyBatisCampRepository  implements CampRepository{

    @Override
    public List<Camp> getCampList() {
        return null;
    }

    @Override
    public Camp getCampOne(String id) {
        return null;
    }

    @Override
    public Optional<Camp> findById(String id) {
        return Optional.empty();
    }

    @Override
    public Optional<Camp> findByDt(String strDt, String endDt) {
        return Optional.empty();
    }

    @Override
    public List<Camp> getDetcChanList() {
        return null;
    }
}
