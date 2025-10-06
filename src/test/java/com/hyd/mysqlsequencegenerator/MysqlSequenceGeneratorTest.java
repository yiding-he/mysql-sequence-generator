package com.hyd.mysqlsequencegenerator;

import com.hyd.mysqlsequencegenerator.MysqlSequenceGenerator.SequenceUpdateListener;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;

public class MysqlSequenceGeneratorTest {

    public static final String URL = "jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=UTC";

    public static final String USERNAME = "root";

    public static final String PASSWORD = "root123";

    @Test
    public void testNextSequence() {
        MysqlSequenceGenerator mysqlSequenceGenerator = createMysqlSequenceGenerator(ECHO_LISTENER);
        for (int i = 0; i < 10; i++) {
            System.out.println(mysqlSequenceGenerator.nextSequence("seq1"));
            System.out.println(mysqlSequenceGenerator.nextSequence("seq2"));
            System.out.println(mysqlSequenceGenerator.nextSequence("seq3"));
        }
    }

    @Test
    public void testOnePerSection() {
        MysqlSequenceGenerator mysqlSequenceGenerator = createMysqlSequenceGenerator(ECHO_LISTENER);
        mysqlSequenceGenerator.updateStep("seq1", 1);
        for (int i = 0; i < 100; i++) {
            System.out.println(mysqlSequenceGenerator.nextSequence("seq1"));
        }
        mysqlSequenceGenerator.updateStep("seq1", 100);
    }

    @Test
    public void testMultiThread() throws Exception {

        // 确保每个 segment 只被分配一次，不论分配是在哪个线程中进行的
        final Set<Long> assignedSegmentMinValues = new HashSet<>();
        final Set<Long> assignedSegmentMaxValues = new HashSet<>();

        MysqlSequenceGenerator mysqlSequenceGenerator = createMysqlSequenceGenerator(update -> {
            assertTrue("segment already assigned: " + update.min, assignedSegmentMinValues.add(update.min));
            assertTrue("segment already assigned: " + update.max, assignedSegmentMaxValues.add(update.max));
            System.out.println("[" + Thread.currentThread().getName() + "] " +
                "Sequence '" + update.sequenceName + "' updated: " + update.min + " ~ " + update.max);
        });

        AtomicLong counter = new AtomicLong(0);
        String sequenceName = "seq1";

        Runnable task = () -> {
            for (int i = 0; i < 5000; i++) {
                try {
                    mysqlSequenceGenerator.nextLong(sequenceName);
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

    @Test
    public void benchmark() throws Exception {
        MysqlSequenceGenerator mysqlSequenceGenerator = createMysqlSequenceGenerator(ECHO_LISTENER);
        AtomicLong counter = new AtomicLong(0);
        String sequenceName = "seq1";

        Runnable task = () -> {
            for (int i = 0; i < 100000; i++) {
                try {
                    mysqlSequenceGenerator.nextLong(sequenceName);
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

    private static final SequenceUpdateListener ECHO_LISTENER = update -> {
        String threadName = Thread.currentThread().getName();
        System.out.println("[" + threadName + "] " +
            "Sequence '" + update.sequenceName + "' updated: " + update.min + " ~ " + update.max);
    };

    private MysqlSequenceGenerator createMysqlSequenceGenerator(SequenceUpdateListener listener) {

        // 准备数据源
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setUrl(URL);
        basicDataSource.setDriverClassName(com.mysql.cj.jdbc.Driver.class.getCanonicalName());
        basicDataSource.setUsername(USERNAME);
        basicDataSource.setPassword(PASSWORD);

        // 构造 MysqlSequenceGenerator 对象
        MysqlSequenceGenerator generator =
            new MysqlSequenceGenerator(basicDataSource);

        // 侦听序列更新事件
        if (listener != null) {
            generator.setOnSequenceUpdate(listener);
        }

        return generator;
    }
}