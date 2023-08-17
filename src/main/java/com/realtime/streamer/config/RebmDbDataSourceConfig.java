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
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

@Configuration
@MapperScan(value = "com.realtime.streamer.mappers.rebm")
public class RebmDbDataSourceConfig {

    private final String REBM_SOURCE = "rebmSource";

    // rebm database DataSource
    @Primary
    @Bean(REBM_SOURCE)
    @ConfigurationProperties(prefix = "spring.rebm.datasource.hikari")
    public DataSource rebmSource() {
        return DataSourceBuilder.create()
                .type(HikariDataSource.class)
                .build();
    }

    @Primary
    @Bean(name= "rebmJdbcTemplate")
    public JdbcTemplate rebmDataConnection(@Qualifier(REBM_SOURCE) DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }

    // SqlSessionTemplate ���� ����� SqlSession �� �����ϴ� Factory
    @Primary
    @Bean
    public SqlSessionFactory REBMSqlSessionFactory(DataSource dataSource) throws Exception {
        /*
         * MyBatis �� JdbcTemplate ��� Connection ��ü�� ���� ���Ǹ� ���ؼ� SqlSession �� ����Ѵ�.
         * ���������� SqlSessionTemplate �� SqlSession �� �����Ѵ�.
         * Thread-Safe �ϰ� ���� ���� Mapper ���� ������ �� �ִ�.
         */
        SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
        bean.setDataSource(dataSource);

        // MyBatis Mapper Source
        // MyBatis �� SqlSession ���� �ҷ��� ���� ����
        Resource[] res = new PathMatchingResourcePatternResolver().getResources("classpath:mappers/rebm/*.xml");
        bean.setMapperLocations(res);

        // MyBatis Config Setting
        // MyBatis ���� ����
        Resource myBatisConfig = new PathMatchingResourcePatternResolver().getResource("classpath:config/MyBatis-Config.xml");
        bean.setConfigLocation(myBatisConfig);

        return bean.getObject();
    }

    // DataSource ���� Transaction ������ ���� Manager Ŭ���� ���
    @Primary
    @Bean
    public DataSourceTransactionManager RebmTransactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}
