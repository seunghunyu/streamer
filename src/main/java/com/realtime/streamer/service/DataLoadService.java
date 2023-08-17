package com.realtime.streamer.service;

import com.realtime.streamer.repository.rebm.JdbcTemplateDataLoadRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class DataLoadService {
    @Autowired
    JdbcTemplateDataLoadRepository repository;

    public void dataLoad(){
        repository.dataLoad();
    }
}
