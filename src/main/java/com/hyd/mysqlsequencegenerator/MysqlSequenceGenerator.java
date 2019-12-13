package com.hyd.mysqlsequencegenerator;

import static java.util.Optional.ofNullable;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public class MysqlSequenceGenerator {

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    public static final String DEFAULT_UPDATE = "update #table# " +
        "set #seqvalue# = last_insert_id(" +
        "  if(#seqvalue# + #step# > #max#, 0, #seqvalue# + #step#)" +
        ") where #seqname# = ?";

    public static final String SEQ_SEGMENT_QUERY = "SELECT " +
        "last_insert_id(), last_insert_id() + #step# FROM #table#";

    public static final String SEQ_CODE_QUERY = "select #code# code, #max# max "
        + "from #table# where #seqname# = ?";

    public static final String DEFAULT_TABLE_NAME = "t_sequence";

    public static final String DEFAULT_SEQ_NAME_COLUMN = "name";

    public static final String DEFAULT_SEQ_CODE_COLUMN = "code";

    public static final String DEFAULT_SEQ_VALUE_COLUMN = "value";

    public static final String DEFAULT_SEQ_STEP_COLUMN = "step";

    public static final String DEFAULT_SEQ_MAX_COLUMN = "max";

    private static final Map<String, SeqInfo> SEQ_INFO_MAP = new ConcurrentHashMap<>();

    ////////////////////////////////////////////////////////////

    @FunctionalInterface
    interface F<A, B> {

        B f(A a) throws SQLException;
    }

    @FunctionalInterface
    interface ConnectionSupplier {

        Connection get() throws SQLException;
    }

    @FunctionalInterface
    interface ConnectionCloser {

        void close(Connection connection) throws SQLException;
    }

    static class SeqInfo {
        private String code;
        private long max;
        private long timestamp = System.currentTimeMillis();
    }

    static class SQLExceptionWrapper extends RuntimeException {

        SQLExceptionWrapper(SQLException cause) {
            super(cause);
        }

        public SQLException getCause() {
            return (SQLException) super.getCause();
        }
    }

    static class Threshold {

        long max;           // max value for current section

        long threshold;     // threshold value for when to fetch new section

        Threshold(long max, long threshold) {
            this.max = max;
            this.threshold = threshold;
        }

        boolean needFetch(long seq) {
            return seq + 1 >= threshold;
        }

        boolean runOut(long seq) {
            return seq + 1 >= max;
        }
    }

    //////////////////////////////////////////////////////////////

    class Counter {

        String sequenceName;

        Threshold threshold;

        AtomicLong value = new AtomicLong();

        volatile Future<Long> future;

        public Counter(String sequenceName, long min, long max) {
            this.sequenceName = sequenceName;
            this.value.set(min);
            this.threshold = new Threshold(max, max);
        }

        public long next() throws SQLException {
            try {
                return value.updateAndGet(seq -> asyncFetch ? nextAsync(seq) : nextSync(seq));
            } catch (SQLExceptionWrapper e) {
                throw e.getCause();
            }
        }

        private long nextAsync(long seq) {
            if (threshold.needFetch(seq) && future == null) {
                synchronized (this) {
                    if (future == null) {
                        future = asyncFetcher.submit(() -> {
                            long[] minMax = updateSequence();
                            long t = (minMax[1] - minMax[0]) / 2 + minMax[0];
                            threshold = new Threshold(minMax[1], t);
                            return minMax[0];
                        });
                    }
                }
            }

            if (threshold.runOut(seq)) {
                synchronized (this) {
                    if (threshold.runOut(seq)) {
                        try {
                            final Long l = future.get();
                            future = null;
                            return l;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        return seq + 1;
                    }
                }
            } else {
                return seq + 1;
            }
        }

        private long nextSync(long seq) {
            if (threshold.needFetch(seq)) {
                synchronized (this) {
                    if (threshold.needFetch(seq)) {
                        long[] minMax = updateSequence();
                        threshold = new Threshold(minMax[1], minMax[1]);
                        return minMax[0];
                    } else {
                        return seq + 1;
                    }
                }
            } else {
                return seq + 1;
            }
        }

        private long[] updateSequence() {
            try {
                return withConnection(connection -> {
                    executeUpdate(connection, sequenceName);
                    return querySegment(connection);
                });
            } catch (SQLException e) {
                throw new SQLExceptionWrapper(e);
            }
        }

        private void debugOutput(long l) {
            sleep(10, 50);
            System.out.println(Thread.currentThread().getName() + ":" + l + ", " + threshold.threshold);
        }
    }

    private static void sleep(int min, int max) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(min, max));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    ////////////////////////////////////////////////////////////

    private ConnectionSupplier connectionSupplier;

    private ConnectionCloser connectionCloser;

    private String updateTemplate;

    private String querySegmentTemplate;

    private String queryCodeTemplate;

    private String seqCodeColumn, seqMaxColumn;

    private Map<String, Counter> counters = new ConcurrentHashMap<>();

    private BiConsumer<Long, Long> onSequenceUpdate;

    /**
     * Whether or not fetch next sequence section with a daemon thread
     */
    private boolean asyncFetch = false;

    private ExecutorService asyncFetcher = Executors.newSingleThreadExecutor();

    /**
     * Constructor.
     *
     * @param connectionSupplier how to get a JDBC Connection object before database operation
     * @param connectionCloser   how to deal with Connection object after database operation
     * @param tableName          (nullable) customized sequence table name
     * @param seqValueColumn     (nullable) customized column name for current sequence value
     * @param seqNameColumn      (nullable) customized column name for sequence name
     * @param seqStepColumn      (nullable) customized column name for sequence step
     * @param seqMaxColumn       (nullable) customized column name for sequence max value
     */
    public MysqlSequenceGenerator(
        ConnectionSupplier connectionSupplier, ConnectionCloser connectionCloser,
        String tableName, String seqValueColumn, String seqNameColumn, String seqCodeColumn,
        String seqStepColumn, String seqMaxColumn
    ) {

        tableName = ofNullable(tableName).orElse(DEFAULT_TABLE_NAME);
        seqNameColumn = ofNullable(seqNameColumn).orElse(DEFAULT_SEQ_NAME_COLUMN);
        seqValueColumn = ofNullable(seqValueColumn).orElse(DEFAULT_SEQ_VALUE_COLUMN);
        seqStepColumn = ofNullable(seqStepColumn).orElse(DEFAULT_SEQ_STEP_COLUMN);
        seqMaxColumn = ofNullable(seqMaxColumn).orElse(DEFAULT_SEQ_MAX_COLUMN);
        seqCodeColumn = ofNullable(seqCodeColumn).orElse(DEFAULT_SEQ_CODE_COLUMN);

        this.connectionSupplier = connectionSupplier;
        this.connectionCloser = connectionCloser;
        this.seqCodeColumn = seqCodeColumn;
        this.seqMaxColumn = seqMaxColumn;

        this.updateTemplate = DEFAULT_UPDATE
            .replace("#table#", tableName)
            .replace("#seqvalue#", seqValueColumn)
            .replace("#step#", seqStepColumn)
            .replace("#seqname#", seqNameColumn)
            .replace("#max#", seqMaxColumn);

        this.querySegmentTemplate = SEQ_SEGMENT_QUERY
            .replace("#table#", tableName)
            .replace("#step#", seqStepColumn);

        this.queryCodeTemplate = SEQ_CODE_QUERY
            .replace("#code#", seqCodeColumn)
            .replace("#max#", seqMaxColumn)
            .replace("#table#", tableName)
            .replace("#seqname#", seqNameColumn);
    }

    public String getUpdateTemplate() {
        return updateTemplate;
    }

    public String getQuerySegmentTemplate() {
        return querySegmentTemplate;
    }

    public String getQueryCodeTemplate() {
        return queryCodeTemplate;
    }

    public void setOnSequenceUpdate(BiConsumer<Long, Long> onSequenceUpdate) {
        this.onSequenceUpdate = onSequenceUpdate;
    }

    public void setAsyncFetch(boolean asyncFetch) {
        this.asyncFetch = asyncFetch;
    }

    ////////////////////////////////////////////////////////////

    private ConnectionSupplier dataSource() {
        return ofNullable(this.connectionSupplier)
            .orElseThrow(() -> new IllegalStateException("connectionSupplier is null"));
    }

    public Long nextLong(String sequenceName) throws SQLException {
        Counter counter = counters.computeIfAbsent(sequenceName, s -> new Counter(s, 0, 0));
        return counter.next();
    }

    public String nextSequence(String sequenceName) throws SQLException {
        Counter counter = counters.computeIfAbsent(sequenceName, s -> new Counter(s, 0, 0));
        Long nextLong = counter.next();

        String today = DATE_FORMATTER.format(LocalDate.now());
        SeqInfo seqInfo = withConnection(conn -> querySeqInfo(conn, sequenceName));
        int length = (int) Math.log10(seqInfo.max) + 1;

        return today + seqInfo.code + String.format("%0" + length + "d", nextLong);
    }

    private <T> T withConnection(F<Connection, T> f) throws SQLException {
        Connection connection = dataSource().get();

        if (connection == null) {
            throw new IllegalStateException("Connection is null");
        }

        try {
            return f.f(connection);
        } finally {
            connectionCloser.close(connection);
        }
    }

    private void executeUpdate(Connection connection, String sequenceName) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(this.updateTemplate)) {
            ps.setString(1, sequenceName);
            ps.executeUpdate();
        }
    }

    private long[] querySegment(Connection connection) throws SQLException {
        try (
            PreparedStatement ps = connection.prepareStatement(this.querySegmentTemplate);
            ResultSet rs = ps.executeQuery()
        ) {
            if (rs.next()) {
                long sectionMin = rs.getBigDecimal(1).longValue();
                long sectionMax = rs.getBigDecimal(2).longValue();

                if (onSequenceUpdate != null) {
                    try {
                        onSequenceUpdate.accept(sectionMin, sectionMax);
                    } catch (Throwable e) {
                        // ignore event handler error
                    }
                }

                return new long[]{sectionMin, sectionMax};
            }
        }
        throw new IllegalStateException("query result is empty");
    }

    private SeqInfo querySeqInfo(Connection connection, String seqName) throws SQLException {
        try (
            PreparedStatement ps = createPs(connection, seqName);
            ResultSet rs = ps.executeQuery()
        ) {
            if (rs.next()) {
                SeqInfo seqInfo = new SeqInfo();
                seqInfo.code = rs.getString(this.seqCodeColumn);
                seqInfo.max = rs.getLong(this.seqMaxColumn);
                return seqInfo;
            }
        }
        throw new IllegalStateException("query result is empty");
    }

    private PreparedStatement createPs(Connection connection, Object... args) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(this.queryCodeTemplate);
        for (int i = 0; i < args.length; i++) {
            ps.setObject(i + 1, args[i]);
        }
        return ps;
    }
}
