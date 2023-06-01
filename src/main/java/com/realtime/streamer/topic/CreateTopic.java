package com.realtime.streamer.topic;

import com.realtime.streamer.execution.GetReadyCamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.rocksdb.util.Environment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
@Slf4j
@RequiredArgsConstructor
@Component //Main 호출시 스캔
//@Order(1) -> ApplicationRunner 순서 정해 줄 수 있음
//public class CreateTopic implements ApplicationRunner {
public class CreateTopic{
    Properties properties;
    Admin admin;

    //application.properties에 정의된 bootstrapServer 주소들
    @Value("${kafka.bootstrapAddress}")
    private String bootstrapServers;

//    @Override
//    public void run(ApplicationArguments args) throws Exception {
//        System.out.println("Application Runner@@@@@@@");
//    }

    @Bean
    public void create() {
        System.out.println("create1 ");
        //프로퍼티 생성
        System.out.println(bootstrapServers);
        Map<String,Object> map = new HashMap<>();
        properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        //kafka.clients.admin 객체 생성
        admin = Admin.create(properties);

        //String 토픽명, int 파티션, (short) replicationFactor
        NewTopic newTopic = new NewTopic("topic1", 1, (short)1);
        try{
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        }catch(Exception e) {
            // e.printStackTrace();
        }

    }

    public void create2(){
        System.out.println("create2 ");
        //String 토픽명, int 파티션, (short) replicationFactor
        NewTopic newTopic = new NewTopic("topic1", 1, (short)1);
        try{
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        }catch(Exception e) {
           // e.printStackTrace();
        }
    }
    //수행 캠페인 정보 가져오기
    @Bean
    public void getRunningCamp(){
        System.out.println("getRunningCamp!!!");
        log.info("getRunningCamp!!!");
        GetReadyCamp getReadyCamp = new GetReadyCamp();
        getReadyCamp.polling();
    }
}
