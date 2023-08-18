package com.realtime.streamer.service;

import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@Transactional
@SpringBootTest
class DataLoadServiceTest {
    @Autowired
    DataLoadService dataLoadService;

    @Test
    void getMemberName(){
        dataLoadService.dataLoad();
    }

    @Test
    void getQryResult(){
        String qry = "select * from member";
        List<?> objects = dataLoadService.selectData(qry, "H2");
        for(int i = 0 ; i < objects.size() ;i++){
            log.info(objects.get(i).toString());
            Map<String, Object> map = (Map<String, Object>) objects.get(i);

            Iterator<String> itr = map.keySet().iterator();
            while (itr.hasNext())
            {
                String key = itr.next();
//                String value = map.get(key);
                log.info("key = {}, valueClass = {}", key, map.get(key));

                if(Long.class.isInstance(map.get(key).getClass())){
                    log.info("정수형 타입");
                }else{
                    log.info("문자열 타입");
                }


            }

        }

    }
}