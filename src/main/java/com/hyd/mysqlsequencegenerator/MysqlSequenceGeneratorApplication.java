package com.hyd.mysqlsequencegenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.Connection;

/**
 * 示例：如何在 Spring Boot 项目中使用 MysqlSequenceGenerator
 */
@SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
@SpringBootApplication
public class MysqlSequenceGeneratorApplication {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlSequenceGeneratorApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(MysqlSequenceGeneratorApplication.class, args);
    }

    /**
     * 如何构建一个 MysqlSequenceGenerator 对象（附如何自定义表名）
     */
    @Bean
    MysqlSequenceGenerator dataSourceSequenceGenerator(
        DataSource dataSource,
        @Value("${seq.table-name}") String tableName
    ) {
        return new MysqlSequenceGenerator(
            dataSource::getConnection, Connection::close,
            tableName, null, null, null, null
        );
    }

    /**
     * 如何构建一个可以兼容 Spring 事务的 MysqlSequenceGenerator 对象
     * 当处于 Spring 事务中时，会使用当前已经获得的数据库连接，而不是再获取新的连接
     * 否则依然从 DataSource 中获取新的数据库连接
     */
    @Bean
    MysqlSequenceGenerator inTransactionSequenceGenerator(
        DataSource dataSource
    ) {
        return new MysqlSequenceGenerator(
            () -> DataSourceUtils.getConnection(dataSource),
            conn -> DataSourceUtils.releaseConnection(conn, dataSource),
            null, null, null, null, null
        );
    }

    ////////////////////////////////////////////////////////////

    /**
     * 如何使用 MysqlSequenceGenerator 对象
     */
    @Autowired
    @Qualifier("dataSourceSequenceGenerator")
    private MysqlSequenceGenerator sequenceGenerator;

    @Bean
    CommandLineRunner commandLineRunner() {
        return args -> {
            LOG.info("Update template: {}", sequenceGenerator.getUpdateTemplate());
            LOG.info("Sequence: {}", sequenceGenerator.nextLong("seq1"));
            LOG.info("Sequence: {}", sequenceGenerator.nextLong("seq1"));
            LOG.info("Sequence: {}", sequenceGenerator.nextLong("seq1"));
            LOG.info("Sequence: {}", sequenceGenerator.nextLong("seq1"));
            LOG.info("Sequence: {}", sequenceGenerator.nextLong("seq1"));
        };
    }
}
