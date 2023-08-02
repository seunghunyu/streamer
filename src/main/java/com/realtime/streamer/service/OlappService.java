package com.realtime.streamer.service;


import com.realtime.streamer.data.Olapp;
import com.realtime.streamer.repository.MyBatisOlappRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/*
*
* 2023.08.01 �ߺ����� ���� ����Ͻ� ����
*
* */
@Service
@RequiredArgsConstructor
public class OlappService {
    @Autowired
    private final MyBatisOlappRepository repository;

    public List<Olapp> getOlappUseList(){
        return repository.getCampOlappList();
    }


}
