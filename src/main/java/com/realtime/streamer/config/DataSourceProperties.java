package com.realtime.streamer.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@Configuration
@EnableConfigurationProperties
public class DataSourceProperties {
//    @Bean(name = "metaDataSource") //name�� ������ �̸����� Bean ���
//    @Qualifier("metaDataSource")   //������ �̸����� ��ü ���� �� �� ����
//    @Primary                       //�ְ� �Ǵ� DataSource ����
//    @ConfigurationProperties(prefix = "spring.datasource.meta")
//    public DataSource metaDataSource(){
//        return DataSourceBuilder.create().type(HikariDataSource.class).build();
//    }
    @Bean
    @ConfigurationProperties
    public DataSource metaDataSource(){
        return DataSourceBuilder.create().type(HikariDataSource.class).build();
    }

//    @Bean(name = "crmDataSource")
//    @Qualifier("crmDataSource")
//    @Primary
//    @ConfigurationProperties(prefix = "spring.datasource.hikari.crm")
//    public DataSource crmDataSource(){
//            return DataSourceBuilder.create().type(HikariDataSource.class).build();
//    }
//
//    @Bean(name = "rebmDataSource")
//    @Qualifier("rebmDataSource")
//    @ConfigurationProperties(prefix = "spring.datasource.hikari.rebm")
//    public DataSource rebmDataSource(){
//        return DataSourceBuilder.create().type(HikariDataSource.class).build();
//    }
//
//    @Bean(name = "martDataSource")
//    @Qualifier("martDataSource")
//    @ConfigurationProperties(prefix = "spring.datasource.hikari.mart")
//    public DataSource martDataSource(){
//        return DataSourceBuilder.create().type(HikariDataSource.class).build();
//    }
}
