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
@Component //Main ȣ��� ��ĵ
//@Order(1) -> ApplicationRunner ���� ���� �� �� ����
//public class CreateTopic implements ApplicationRunner {
public class CreateTopic{
    Properties properties;
    Admin admin;

    //application.properties�� ���ǵ� bootstrapServer �ּҵ�
    @Value("${kafka.bootstrapAddress}")
    private String bootstrapServers;

//    @Override
//    public void run(ApplicationArguments args) throws Exception {
//        System.out.println("Application Runner@@@@@@@");
//    }

    @Bean
    public void create() {
        System.out.println("create1 ");
        //������Ƽ ����
        System.out.println(bootstrapServers);
        Map<String,Object> map = new HashMap<>();
        properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        //kafka.clients.admin ��ü ����
        admin = Admin.create(properties);

        //String ���ȸ�, int ��Ƽ��, (short) replicationFactor
        NewTopic newTopic = new NewTopic("topic1", 1, (short)1);
        try{
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        }catch(Exception e) {
            // e.printStackTrace();
        }

    }

    public void create2(){
        System.out.println("create2 ");
        //String ���ȸ�, int ��Ƽ��, (short) replicationFactor
        NewTopic newTopic = new NewTopic("topic1", 1, (short)1);
        try{
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        }catch(Exception e) {
           // e.printStackTrace();
        }
    }
    //���� ķ���� ���� ��������
    @Bean
    public void getRunningCamp(){
        System.out.println("getRunningCamp!!!");
        log.info("getRunningCamp!!!");
        GetReadyCamp getReadyCamp = new GetReadyCamp();
        getReadyCamp.polling();
    }
}
