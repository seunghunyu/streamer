package com.realtime.streamer.service;

import com.realtime.streamer.repository.rebm.JdbcTemplateDataLoadRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class DataLoadService {
    @Autowired
    JdbcTemplateDataLoadRepository repository;

    public void dataLoad(){
        repository.dataLoad();
    }

    public void createTable(String qry){
        repository.createTable(qry);
    };
    public void createIndex(String qry){
        repository.createIndex(qry);
    };
    public void insertData(String qry, List<Object[]> list){
        repository.insertData(qry, list);
    };
    public List<?> selectData(String qry, String dbPool){
        return repository.selectData(qry, dbPool);
    };

}
