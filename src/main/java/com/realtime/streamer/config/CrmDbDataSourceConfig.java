package com.realtime.streamer.config;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

@Configuration
@MapperScan(value = "com.realtime.streamer.mappers.crm", sqlSessionFactoryRef = "CrmSqlSessionFactory")

public class CrmDbDataSourceConfig {

    private final String CRM_SOURCE = "crmSource";


    // crm database DataSource
    @Bean(CRM_SOURCE)
    @ConfigurationProperties(prefix = "spring.crm.datasource.hikari")
    public DataSource BDataSource() {
        return DataSourceBuilder.create()
                .type(HikariDataSource.class)
                .build();
    }

    // SqlSessionTemplate ���� ����� SqlSession �� �����ϴ� Factory
    @Bean
    public SqlSessionFactory CrmSqlSessionFactory(@Qualifier(CRM_SOURCE) DataSource dataSource) throws Exception {
        /*
         * MyBatis �� JdbcTemplate ��� Connection ��ü�� ���� ���Ǹ� ���ؼ� SqlSession �� ����Ѵ�.
         * ���������� SqlSessionTemplate �� SqlSession �� �����Ѵ�.
         * Thread-Safe �ϰ� ���� ���� Mapper ���� ������ �� �ִ�.
         */
        SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
        bean.setDataSource(dataSource);

        // MyBatis Mapper Source
        // MyBatis �� SqlSession ���� �ҷ��� ���� ����
        Resource[] res = new PathMatchingResourcePatternResolver().getResources("classpath:mappers/crm/*.xml");
        bean.setMapperLocations(res);

        // MyBatis Config Setting
        // MyBatis ���� ����
        Resource myBatisConfig = new PathMatchingResourcePatternResolver().getResource("classpath:config/mybatis-config.xml");
        bean.setConfigLocation(myBatisConfig);

        return bean.getObject();
    }

    // DataSource ���� Transaction ������ ���� Manager Ŭ���� ���
    @Bean
    public DataSourceTransactionManager CrmTransactionManager(@Qualifier(CRM_SOURCE) DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}