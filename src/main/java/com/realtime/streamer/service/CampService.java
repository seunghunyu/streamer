package com.realtime.streamer.service;


import com.realtime.streamer.data.Camp;
import com.realtime.streamer.repository.JdbcTemplateCampRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class CampService {

    @Autowired
    private final JdbcTemplateCampRepository repository;

    public Optional<Camp> findCampById(String campId){
        return repository.findById(campId);
    }
    public Optional<Camp> findCampByDt(String campStrDt, String campEndDt){
        return repository.findByDt(campStrDt, campEndDt);
    }

}
