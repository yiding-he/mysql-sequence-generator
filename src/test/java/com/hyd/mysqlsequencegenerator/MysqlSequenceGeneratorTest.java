package com.hyd.mysqlsequencegenerator;

import com.mysql.jdbc.Driver;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MysqlSequenceGeneratorTest {

    public static final String URL = "jdbc:mysql://localhost:3306/demo?useSSL=false";

    public static final String USERNAME = "root";

    public static final String PASSWORD = "root123";

    @Test
    public void testGetSequence() throws Exception {
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setUrl(URL);
        basicDataSource.setDriverClassName(Driver.class.getCanonicalName());
        basicDataSource.setUsername(USERNAME);
        basicDataSource.setPassword(PASSWORD);

        MysqlSequenceGenerator mysqlSequenceGenerator =
            new MysqlSequenceGenerator(basicDataSource, true, null, null, null, null, null);

        mysqlSequenceGenerator.setOnSequenceUpdate((min, max) ->
            System.out.println("Sequence section updated: " + min + " ~ " + max));

        Runnable task = () -> {
            for (int i = 0; i < 1000000; i++) {
                try {
                    mysqlSequenceGenerator.nextLong("seq1");
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }
            }
        };

        List<Thread> threads = new ArrayList<>();
        long start = System.currentTimeMillis();

        for (int i = 0; i < 50; i++) {
            Thread thread = new Thread(task);
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        System.out.println("time : " + (System.currentTimeMillis() - start));
    }
}