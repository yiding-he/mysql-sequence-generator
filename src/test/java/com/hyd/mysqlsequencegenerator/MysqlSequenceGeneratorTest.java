package com.hyd.mysqlsequencegenerator;

import com.mysql.jdbc.Driver;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;

import java.io.FileWriter;
import java.sql.Connection;
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
            new MysqlSequenceGenerator(
                basicDataSource::getConnection, Connection::close,
                null, null, null, null, null
            );

        mysqlSequenceGenerator.setOnSequenceUpdate((min, max) ->
            System.out.println(Thread.currentThread().getName() + " Sequence section updated: " + min + " ~ " + max));

        List<Long> values = new ArrayList<>();

        Runnable task = () -> {
            for (int i = 0; i < 50; i++) {
                try {
                    Long seqValue = mysqlSequenceGenerator.nextLong("seq1");
                    values.add(seqValue);
                    System.out.println(Thread.currentThread().getName() + ": [" + seqValue + "]");
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }
            }
        };

        List<Thread> threads = new ArrayList<>();
        long start = System.currentTimeMillis();

        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(task);
            thread.setName(String.format("Counter%02d", i));
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        try(FileWriter fileWriter = new FileWriter("values.txt")) {
            for (Long value : values) {
                fileWriter.write(value + "\n");
            }
            fileWriter.flush();
        }
    }
}