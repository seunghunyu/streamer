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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

@Configuration
@MapperScan(value = "com.realtime.streamer.mappers.h2", sqlSessionFactoryRef = "H2SqlSessionFactory")
public class H2DbDataSourceConfig {

    private final String H2_SOURCE = "h2Source";


    // crm database DataSource
    @Bean(H2_SOURCE)
    @ConfigurationProperties(prefix = "spring.h2.datasource.hikari")
    public DataSource h2Source() {
        return DataSourceBuilder.create()
                .type(HikariDataSource.class)
                .build();
    }

    @Bean(name = "h2JdbcTemplate")
    public JdbcTemplate h2DataConnection(@Qualifier(H2_SOURCE) DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }

    // SqlSessionTemplate 에서 사용할 SqlSession 을 생성하는 Factory
    @Bean
    public SqlSessionFactory H2SqlSessionFactory(@Qualifier(H2_SOURCE) DataSource dataSource) throws Exception {

        SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
        bean.setDataSource(dataSource);

        // MyBatis Mapper Source
        // MyBatis 의 SqlSession 에서 불러올 쿼리 정보
        Resource[] res = new PathMatchingResourcePatternResolver().getResources("classpath:mappers/h2/*.xml");
        bean.setMapperLocations(res);

        // MyBatis Config Setting
        // MyBatis 설정 파일
        Resource myBatisConfig = new PathMatchingResourcePatternResolver().getResource("classpath:config/mybatis-config.xml");
        bean.setConfigLocation(myBatisConfig);

        return bean.getObject();
    }

    // DataSource 에서 Transaction 관리를 위한 Manager 클래스 등록
    @Bean
    public DataSourceTransactionManager H2TransactionManager(@Qualifier(H2_SOURCE) DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}