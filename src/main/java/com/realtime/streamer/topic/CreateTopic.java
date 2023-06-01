package com.realtime.streamer.topic;

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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;

import java.io.IOException;
import java.util.*;

public class CreateTopic {
    Properties properties;
    Admin admin;

    //application.properties�� ���ǵ� bootstrapServer �ּҵ�
    @Value("${kafka.bootstrapAddress}")
    private String bootstrapServers;

    public CreateTopic() {
        //������Ƽ ����
        System.out.println(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
        System.out.println(bootstrapServers);
        Map<String,Object> map = new HashMap<>();
        properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        //kafka.clients.admin ��ü ����
        admin = Admin.create(properties);

    }

    public void create(){
        //String ���ȸ�, int ��Ƽ��, (short) replicationFactor
        NewTopic newTopic = new NewTopic("topic1", 3, (short)3);
        try{
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

}
