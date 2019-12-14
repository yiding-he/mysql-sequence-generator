package com.hyd.mysqlsequencegenerator;

import com.hyd.mysqlsequencegenerator.MysqlSequenceGenerator.Column;
import com.hyd.mysqlsequencegenerator.MysqlSequenceGenerator.ColumnInfo;
import com.mysql.jdbc.Driver;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;

public class MysqlSequenceGeneratorTest {

    public static final String URL = "jdbc:mysql://localhost:3306/demo?useSSL=false";

    public static final String USERNAME = "root";

    public static final String PASSWORD = "root123";

    @Test
    public void testNextSequence() throws Exception {
        MysqlSequenceGenerator mysqlSequenceGenerator = createMysqlSequenceGenerator();
        for (int i = 0; i < 10; i++) {
            System.out.println(mysqlSequenceGenerator.nextSequence("seq1"));
            System.out.println(mysqlSequenceGenerator.nextSequence("seq2"));
            System.out.println(mysqlSequenceGenerator.nextSequence("seq3"));
        }
    }

    @Test
    public void benchmark() throws Exception {
        MysqlSequenceGenerator mysqlSequenceGenerator = createMysqlSequenceGenerator();
        AtomicLong counter = new AtomicLong(0);

        Runnable task = () -> {
            for (int i = 0; i < 100000; i++) {
                try {
                    mysqlSequenceGenerator.nextLong("seq1");
                    counter.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            }
        };

        List<Thread> threads = new ArrayList<>();
        long start = System.currentTimeMillis();

        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(task);
            thread.setName(String.format("Counter%02d", i));
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        long duration = System.currentTimeMillis() - start;
        System.out.println("count: " + counter.get() + ", duration: " + duration);
    }

    private MysqlSequenceGenerator createMysqlSequenceGenerator() {

        // 准备数据源
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setUrl(URL);
        basicDataSource.setDriverClassName(Driver.class.getCanonicalName());
        basicDataSource.setUsername(USERNAME);
        basicDataSource.setPassword(PASSWORD);

        // 构造 MysqlSequenceGenerator 对象
        MysqlSequenceGenerator mysqlSequenceGenerator =
            new MysqlSequenceGenerator(
                basicDataSource::getConnection, Connection::close,
                null, false,
                Collections.singletonList(
                    ColumnInfo.undefined(Column.Min)
                )
            );

        // 侦听序列更新事件
        mysqlSequenceGenerator.setOnSequenceUpdate((min, max) ->
            System.out.println(Thread.currentThread().getName() + " Sequence section updated: " + min + " ~ " + max));

        return mysqlSequenceGenerator;
    }
}