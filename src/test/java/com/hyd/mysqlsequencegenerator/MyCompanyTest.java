package com.hyd.mysqlsequencegenerator;

import com.hyd.mysqlsequencegenerator.MysqlSequenceGenerator.Column;
import com.hyd.mysqlsequencegenerator.MysqlSequenceGenerator.ColumnInfo;
import com.mysql.jdbc.Driver;
import java.sql.Connection;
import java.util.Arrays;
import org.apache.commons.dbcp2.BasicDataSource;

// 测试对公司环境的兼容性
public class MyCompanyTest {

    public static void main(String[] args) {
        MysqlSequenceGenerator generator = createMysqlSequenceGenerator();
        for (int i = 0; i < 110; i++) {
            System.out.println(generator.nextSequence("seq_t_fund_transfer_message", "041"));
        }
    }

    private static MysqlSequenceGenerator createMysqlSequenceGenerator() {

        // 准备数据源
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setUrl("jdbc:mysql://172.16.10.40:3306/frxs_fund");
        basicDataSource.setDriverClassName(Driver.class.getCanonicalName());
        basicDataSource.setUsername("root");
        basicDataSource.setPassword("123456");

        // 构造 MysqlSequenceGenerator 对象
        MysqlSequenceGenerator mysqlSequenceGenerator =
            new MysqlSequenceGenerator(
                basicDataSource::getConnection, Connection::close,
                "t_sequence", false,
                Arrays.asList(
                    ColumnInfo.customName(Column.Name, "name"),
                    ColumnInfo.customName(Column.Value, "value"),
                    ColumnInfo.customName(Column.Min, "min_value"),
                    ColumnInfo.customName(Column.Max, "max_value"),
                    ColumnInfo.customName(Column.Step, "step"),
                    ColumnInfo.undefined(Column.Code)
                )
            );

        // 侦听序列更新事件
        mysqlSequenceGenerator.setOnSequenceUpdate((min, max) ->
            System.out.println(Thread.currentThread().getName() + " Sequence section updated: " + min + " ~ " + max));

        return mysqlSequenceGenerator;
    }

}
