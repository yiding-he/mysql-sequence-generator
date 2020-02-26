package com.hyd.mysqlsequencegenerator;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static java.util.Optional.ofNullable;

/**
 * https://github.com/yiding-he/mysql-sequence-generator
 */
public class MysqlSequenceGenerator {

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    public static final String DEFAULT_UPDATE = "update #table# " +
        "set #seqvalue# = last_insert_id(" +
        "  if(#seqvalue# + #step# > #max#, #min#, #seqvalue# + #step#)" +
        ") where #seqname# = ?";

    public static final String SEQ_SEGMENT_QUERY = "SELECT " +
        "last_insert_id(), last_insert_id() + #step# FROM #table# " +
        "where #seqname# = ?";

    private static final String CODE_COLUMN = "#code# code,";

    public static final String SEQ_CODE_QUERY = "select {{CODE_COLUMN}} #max# "
        + "from #table# where #seqname# = ?";

    public static final String DEFAULT_TABLE_NAME = "t_sequence";

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

        private final String code;

        private final long max;

        public SeqInfo(String code, long max) {
            this.code = code;
            this.max = max;
        }
    }

    static class Threshold {

        final long max;           // max value for current section

        final long threshold;     // threshold value for when to fetch new section

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

    /**
     * Customizable columns
     */
    public enum Column {
        Name("name"),
        Code("code"),
        Value("value"),
        Min("min"),
        Max("max"),
        Step("step");

        private String defaultColumnName;

        Column(String defaultColumnName) {
            this.defaultColumnName = defaultColumnName;
        }

        public String getDefaultColumnName() {
            return defaultColumnName;
        }
    }

    /**
     * Column info after customization
     */
    public static class ColumnInfo {

        private final Column column;

        private final String value;

        private ColumnInfo(Column column, String value) {
            this.column = column;
            this.value = value;
        }

        public static ColumnInfo undefined(Column c) {
            return new ColumnInfo(c, null);
        }

        public static ColumnInfo defaultName(Column c) {
            return new ColumnInfo(c, c.getDefaultColumnName());
        }

        public static ColumnInfo customName(Column c, String name) {
            return new ColumnInfo(c, name);
        }
    }

    public static class MysqlSequenceException extends RuntimeException {

        public MysqlSequenceException(Throwable cause) {
            super(cause);
        }

        public MysqlSequenceException(String message) {
            super(message);
        }
    }

    //////////////////////////////////////////////////////////////

    /**
     * 每个序列对应一个 Counter 对象
     * 注意：对会被多线程访问的成员，要么是 final 的，要么就必须是 volatile 的。
     */
    class Counter {

        final String sequenceName;                  // 序列名称

        final AtomicLong value = new AtomicLong();  // 序列的当前值

        volatile Threshold threshold;    // 当前队列的阈值，用来判断是否该从数据库取下一个序列段

        volatile Future<Long> future;    // 如果是异步取下一序列段，则要用到 Future

        public Counter(String sequenceName, long min, long max) {
            this.sequenceName = sequenceName;
            this.value.set(min);
            this.threshold = new Threshold(max, max);
        }

        public long next() throws MysqlSequenceException {
            try {
                // updateAndGet() 方法能够保证操作的原子性
                return value.updateAndGet(seq -> asyncFetch ? nextAsync(seq) : nextSync(seq));
            } catch (Exception e) {
                throw new MysqlSequenceException(e);
            }
        }

        /**
         * 取下一个序号，如果需要的话，以异步方式从数据库取新的序号段
         *
         * @param seq 当前序号
         */
        private long nextAsync(long seq) {

            // 判断是否该取序号段了，以及是否正在取序号段
            if (threshold.needFetch(seq) && future == null) {
                synchronized (this) {
                    // 进入这里但发现不满足，意味着：1）threshold 已经更新，或2）有其他线程已经开始取了
                    if (threshold.needFetch(seq) && future == null) {
                        future = asyncFetcher.submit(() -> {
                            long[] minMax = updateSequence();
                            // 除以 2 的意思是当序号段用掉一半时，触发异步取下一个序号段
                            long t = (minMax[1] - minMax[0]) / 2 + minMax[0];
                            threshold = new Threshold(minMax[1], t);
                            return minMax[0];
                        });
                    }
                }
            }

            // 如果当前序号段仍然可用，则取当前序号段下一个值，
            // 否则从 future.get() 取值。注意：
            // 1、future.get() 会阻塞；
            // 2、future.get() 成功完成时，threshold 已经被更新
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

        /**
         * 取下一个序号，如果需要的话，以同步方式从数据库取新的序号段
         *
         * @param seq 当前序号
         */
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

        /**
         * 从数据库取下一个序列段，并返回该段的最小值和最大值
         */
        private long[] updateSequence() {
            return withConnection(connection -> {
                executeUpdate(connection, sequenceName);
                return querySegment(connection, sequenceName);
            });
        }
    }

    ////////////////////////////////////////////////////////////

    private final ConnectionSupplier connectionSupplier;

    private final ConnectionCloser connectionCloser;

    private final String updateTemplate;

    private final String querySegmentTemplate;

    private final String queryCodeTemplate;

    private final Map<String, Counter> counters = new ConcurrentHashMap<>();

    private final Map<String, SeqInfo> seqInfoMap = new ConcurrentHashMap<>();

    private final Map<Column, ColumnInfo> columnInfoMap;  // set by user

    private final boolean asyncFetch;

    private final ExecutorService asyncFetcher = new ThreadPoolExecutor(
        1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()
    );

    private BiConsumer<Long, Long> onSequenceUpdate;

    //////////////////////////////////////////////////////////////

    public static class Config {

        private String tableName = DEFAULT_TABLE_NAME;

        private boolean asyncFetch;

        private List<ColumnInfo> columnInfos = new ArrayList<>();

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public boolean isAsyncFetch() {
            return asyncFetch;
        }

        public void setAsyncFetch(boolean asyncFetch) {
            this.asyncFetch = asyncFetch;
        }

        public List<ColumnInfo> getColumnInfos() {
            return columnInfos;
        }

        public void setColumnInfos(List<ColumnInfo> columnInfos) {
            this.columnInfos = columnInfos;
        }
    }

    /**
     * Constructor.
     *
     * @param dataSource DataSource object
     */
    public MysqlSequenceGenerator(DataSource dataSource) {
        this(dataSource::getConnection, Connection::close, new Config());
    }

    /**
     * Constructor.
     *
     * @param dataSource DataSource object
     */
    public MysqlSequenceGenerator(DataSource dataSource, Config config) {
        this(dataSource::getConnection, Connection::close, config);
    }

    /**
     * Constructor.
     *
     * @param connectionSupplier how to get a JDBC Connection object before database operation
     */
    public MysqlSequenceGenerator(ConnectionSupplier connectionSupplier) {
        this(connectionSupplier, Connection::close, new Config());
    }

    /**
     * Constructor.
     *
     * @param connectionSupplier how to get a JDBC Connection object before database operation
     * @param config             customizations
     */
    public MysqlSequenceGenerator(ConnectionSupplier connectionSupplier, Config config) {
        this(connectionSupplier, Connection::close, config);
    }

    /**
     * Constructor.
     *
     * @param connectionSupplier how to get a JDBC Connection object before database operation
     * @param connectionCloser   how to deal with Connection object after database operation
     * @param config             customizations
     */
    public MysqlSequenceGenerator(
        ConnectionSupplier connectionSupplier, ConnectionCloser connectionCloser, Config config
    ) {
        this(connectionSupplier, connectionCloser, config.tableName, config.asyncFetch, config.columnInfos);
    }

    /**
     * Constructor.
     *
     * @param connectionSupplier how to get a JDBC Connection object before database operation
     * @param connectionCloser   how to deal with Connection object after database operation
     * @param tableName          (nullable) customized sequence table name
     * @param asyncFetch         whether to fetch new segment asynchronously
     * @param columnInfos        column customizations
     */
    public MysqlSequenceGenerator(
        ConnectionSupplier connectionSupplier, ConnectionCloser connectionCloser,
        String tableName, boolean asyncFetch, List<ColumnInfo> columnInfos
    ) {

        // Create and fill this.columnInfoMap
        Map<Column, ColumnInfo> cmap = new HashMap<>();
        columnInfos.forEach(info -> cmap.put(info.column, info));
        for (Column c : Column.values()) {
            if (!cmap.containsKey(c)) {
                cmap.put(c, ColumnInfo.defaultName(c));
            }
        }
        this.columnInfoMap = cmap;

        // Prepare SQL templates
        tableName = ofNullable(tableName).orElse(DEFAULT_TABLE_NAME);
        String seqNameColumn = cmap.get(Column.Name).value;
        String seqCodeColumn = cmap.get(Column.Code).value;
        String seqValueColumn = cmap.get(Column.Value).value;
        String seqMinColumn = cmap.get(Column.Min).value;
        String seqMaxColumn = cmap.get(Column.Max).value;
        String seqStepColumn = cmap.get(Column.Step).value;

        this.connectionSupplier = connectionSupplier;
        this.connectionCloser = connectionCloser;

        this.updateTemplate = DEFAULT_UPDATE
            .replace("#table#", tableName)
            .replace("#seqvalue#", seqValueColumn)
            .replace("#step#", seqStepColumn)
            .replace("#seqname#", seqNameColumn)
            .replace("#min#", getColumnName(Column.Min) == null ? "0" : seqMinColumn)
            .replace("#max#", seqMaxColumn);

        this.querySegmentTemplate = SEQ_SEGMENT_QUERY
            .replace("#table#", tableName)
            .replace("#seqname#", seqNameColumn)
            .replace("#step#", seqStepColumn);

        this.queryCodeTemplate = SEQ_CODE_QUERY
            .replace("{{CODE_COLUMN}}", getColumnName(Column.Code) == null ? "" : CODE_COLUMN)
            .replace("#code#", getColumnName(Column.Code) == null ? "" : seqCodeColumn)
            .replace("#max#", seqMaxColumn)
            .replace("#table#", tableName)
            .replace("#seqname#", seqNameColumn);

        this.asyncFetch = asyncFetch;
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

    ////////////////////////////////////////////////////////////

    /**
     * Get next numeric value of sequence
     */
    public Long nextLong(String sequenceName) throws MysqlSequenceException {
        Counter counter = counters.computeIfAbsent(sequenceName, s -> new Counter(s, 0, 0));
        return counter.next();
    }

    /**
     * Get a string sequence with formatting: yyyyMMdd+code+seq
     */
    public String nextSequence(String sequenceName) throws MysqlSequenceException {
        return nextSequence(sequenceName, null);
    }

    /**
     * Get a string sequence with formatting: yyyyMMdd+code+seq
     */
    public String nextSequence(String sequenceName, String code) throws MysqlSequenceException {
        Counter counter = counters.computeIfAbsent(sequenceName, s -> new Counter(s, 0, 0));
        Long nextLong = counter.next();

        String today = DATE_FORMATTER.format(LocalDate.now());
        boolean hasCode = code == null;
        SeqInfo seqInfo = withConnection(conn -> querySeqInfo(conn, sequenceName, hasCode));
        int length = (int) Math.log10(seqInfo.max) + 1;

        return today + (code != null ? code : seqInfo.code) + String.format("%0" + length + "d", nextLong);
    }

    //////////////////////////////////////////////////////////////

    private String getColumnName(Column c) {
        return columnInfoMap.containsKey(c) ? columnInfoMap.get(c).value : null;
    }

    private ConnectionSupplier dataSource() {
        return ofNullable(this.connectionSupplier)
            .orElseThrow(() -> new IllegalStateException("connectionSupplier is null"));
    }

    private <T> T withConnection(F<Connection, T> f) throws MysqlSequenceException {
        try {
            Connection connection = dataSource().get();

            if (connection == null) {
                throw new IllegalStateException("Connection is null");
            }

            try {
                return f.f(connection);
            } finally {
                connectionCloser.close(connection);
            }
        } catch (Exception e) {
            throw new MysqlSequenceException(e);
        }
    }

    private void executeUpdate(Connection connection, String sequenceName) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(this.updateTemplate)) {
            ps.setString(1, sequenceName);
            ps.executeUpdate();
        }
    }

    private long[] querySegment(Connection connection, String sequenceName) throws SQLException {
        // System.out.println("开始取新的序列段...");
        // __sleep__(1000);
        try (PreparedStatement ps = connection.prepareStatement(this.querySegmentTemplate)) {
            ps.setString(1, sequenceName);
            try (ResultSet rs = ps.executeQuery()) {
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
        }
        throw new IllegalStateException("query result is empty");
    }

    private SeqInfo querySeqInfo(Connection connection, String seqName, boolean hasCode) throws SQLException {
        return seqInfoMap.computeIfAbsent(seqName, __seqName__ -> {
            // __sleep__(1000);
            try {
                try (
                    PreparedStatement ps = createPs(connection, this.queryCodeTemplate, __seqName__);
                    ResultSet rs = ps.executeQuery()
                ) {
                    if (rs.next()) {
                        return new SeqInfo(
                            hasCode ? rs.getString(getColumnName(Column.Code)) : null,
                            rs.getLong(getColumnName(Column.Max))
                        );
                    } else {
                        throw new MysqlSequenceException("query result is empty");
                    }
                }
            } catch (SQLException e) {
                throw new MysqlSequenceException(e);
            }
        });
    }

    private PreparedStatement createPs(Connection connection, String sql, Object... args) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i = 0; i < args.length; i++) {
            ps.setObject(i + 1, args[i]);
        }
        return ps;
    }

    ////////////////////////////////////////////////////////////// debugging

    private static final void __sleep__(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // ignore this error
        }
    }

    private static final void __output__(String message) {
        System.out.println(message + " [" + Thread.currentThread().getName() + "]");
    }

    private static final void __assert__(boolean b) {
        if (!b) {
            throw new IllegalStateException("Assert failed");
        }
    }
}
