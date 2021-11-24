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
import java.sql.SQLException;
import java.util.Collections;

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

    /////////////////////////////////////////////////////////////////// 示例：如何构建 MysqlSequenceGenerator 对象

    /**
     * 示例1：构建一个 MysqlSequenceGenerator 对象（附如何自定义表名）
     */
    @Bean
    MysqlSequenceGenerator dataSourceSequenceGenerator(
        DataSource dataSource,
        @Value("${seq.table-name}") String tableName
    ) {
        return new MysqlSequenceGenerator(
            dataSource::getConnection, Connection::close,
            tableName, false, Collections.emptyList()
        );
    }

    /**
     * 示例2：构建一个可以兼容 Spring 事务的 MysqlSequenceGenerator 对象
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
            null, false, Collections.emptyList()
        );
    }

    //////////////////////////////////////////////////////////// 示例：如何使用 MysqlSequenceGenerator 对象

    /**
     * 如何使用 MysqlSequenceGenerator 对象
     */
    @Autowired
    @Qualifier("dataSourceSequenceGenerator")
    private MysqlSequenceGenerator sequenceGenerator;

    @Bean
    CommandLineRunner commandLineRunner() {
        return args -> {
            initTable();

            LOG.info("Update template: {}", sequenceGenerator.getUpdateTemplate());
            LOG.info("Sequence: {}", sequenceGenerator.nextLong("seq1"));
            LOG.info("Sequence: {}", sequenceGenerator.nextLong("seq1"));
            LOG.info("Sequence: {}", sequenceGenerator.nextLong("seq1"));
            LOG.info("String Sequence: {}", sequenceGenerator.nextSequence("seq1"));
            LOG.info("String Sequence: {}", sequenceGenerator.nextSequence("seq1"));
            LOG.info("String Sequence: {}", sequenceGenerator.nextSequence("seq1"));
        };
    }

    ///////////////////////////////////////////////////////////////////

    @Autowired
    private DataSource dataSource;

    private void initTable() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.createStatement().execute(
                "create database if not exists test"
            );
            connection.createStatement().execute(
                "create table if not exists test.t_sequence(\n" +
                    "  name  varchar(100) primary key             comment '序列名称',\n" +
                    "  code  varchar(5) not null default '00000'  comment '序列编号（嵌入最终序列中）',\n" +
                    "  value bigint     not null default 0        comment '当前值',\n" +
                    "  min   bigint     not null default 0        comment '最小值',\n" +
                    "  max   bigint     not null default 99999999 comment '最大值，当 value 达到最大值时重新从 min 开始',\n" +
                    "  step  int        not null default 1000     comment '步长，每次取序列时缓存多少，步长越大，数据库访问频率越低'\n" +
                    ")"
            );
            connection.createStatement().execute(
                "REPLACE into test.t_sequence (name, code, max) values ('seq1', '886', 99999999)"
            );
            LOG.info("Table created.");
        }
    }
}
