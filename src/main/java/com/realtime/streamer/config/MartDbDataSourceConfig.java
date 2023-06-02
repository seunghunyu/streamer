package com.realtime.streamer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
//@Configuration
/*
  @MapperScan SpringBoot 3.x 부터 지원
 */
//@MapperScan(value = "com.example.demo.test1.repository", sqlSessionFactoryRef = "factory")
public class MartDbDataSourceConfig {
//    @Primary
//    @Bean(name = "datasource")
//    @ConfigurationProperties(prefix = "spring.datasource.test1")
//    public DataSource dataSource() {
//        return DataSourceBuilder.create().build();
//    }
//
//    @Primary
//    @Bean(name = "factory")
//    public SqlSessionFactory sqlSessionFactory(DataSource dataSource) throws Exception {
//        SqlSessionFactoryBean sqlSessionFactory = new SqlSessionFactoryBean();
//        sqlSessionFactory.setDataSource(dataSource);
//        sqlSessionFactory.setTypeAliasesPackage("com.example.demo.test1");
//        sqlSessionFactory.setMapperLocations(new PathMatchingResourcePatternResolver().getResources("classpath:/mapper1/*.xml"));
//        return sqlSessionFactory.getObject();
//    }
//
//    @Primary
//    @Bean(name = "sqlSession")
//    public SqlSessionTemplate sqlSession(SqlSessionFactory sqlSessionFactory) {
//        return new SqlSessionTemplate(sqlSessionFactory);
//    }
}
