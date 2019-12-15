package com.hyd.mysqlsequencegenerator;

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

public class MysqlSequenceGenerator {

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    public static final String DEFAULT_UPDATE = "update #table# " +
        "set #seqvalue# = last_insert_id(" +
        "  if(#seqvalue# + #step# > #max#, #min#, #seqvalue# + #step#)" +
        ") where #seqname# = ?";

    public static final String SEQ_SEGMENT_QUERY = "SELECT " +
        "last_insert_id(), last_insert_id() + #step# FROM #table#";

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

        private String code;

        private long max;
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

    /**
     * Customizable columns
     */
    public enum Column {
        Name("name"), Code("code"), Value("value"),
        Min("min"), Max("max"), Step("step");

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

        private Column column;

        private String value = null;

        // Specify if a column is missing in actual sequence table
        // For now only `min` and `code` are supported
        public static ColumnInfo undefined(Column c) {
            ColumnInfo ci = new ColumnInfo();
            ci.column = c;
            ci.value = null;
            return ci;
        }

        public static ColumnInfo defaultName(Column c) {
            ColumnInfo ci = new ColumnInfo();
            ci.column = c;
            ci.value = c.getDefaultColumnName();
            return ci;
        }

        // Specify if you have a different column name
        public static ColumnInfo customName(Column c, String name) {
            ColumnInfo ci = new ColumnInfo();
            ci.column = c;
            ci.value = name;
            return ci;
        }
    }

    public static class MysqlSequenceException extends RuntimeException {

        public MysqlSequenceException(Throwable cause) {
            super(cause);
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

        public long next() throws MysqlSequenceException {
            try {
                return value.updateAndGet(seq -> asyncFetch ? nextAsync(seq) : nextSync(seq));
            } catch (Exception e) {
                throw new MysqlSequenceException(e);
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
            return withConnection(connection -> {
                executeUpdate(connection, sequenceName);
                return querySegment(connection);
            });
        }
    }

    ////////////////////////////////////////////////////////////

    private ConnectionSupplier connectionSupplier;

    private ConnectionCloser connectionCloser;

    private String updateTemplate;

    private String querySegmentTemplate;

    private String queryCodeTemplate;

    private Map<String, Counter> counters = new ConcurrentHashMap<>();

    private BiConsumer<Long, Long> onSequenceUpdate;

    /**
     * Whether or not fetch next sequence section with a daemon thread
     */
    private boolean asyncFetch;

    private Map<Column, ColumnInfo> columnInfoMap;

    private ExecutorService asyncFetcher = Executors.newSingleThreadExecutor();

    //////////////////////////////////////////////////////////////

    public static class Config {

        private String tableName;

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
     * @param connectionSupplier how to get a JDBC Connection object before database operation
     * @param config             configurations
     */
    public MysqlSequenceGenerator(ConnectionSupplier connectionSupplier, Config config
    ) {
        this(connectionSupplier, Connection::close, config);
    }

    /**
     * Constructor.
     *
     * @param connectionSupplier how to get a JDBC Connection object before database operation
     * @param connectionCloser   how to deal with Connection object after database operation
     * @param config             configurations
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
     * @param columnInfos        columns configuration
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
            .replace("#step#", seqStepColumn);

        this.queryCodeTemplate = SEQ_CODE_QUERY
            .replace("{{CODE_COLUMN}}", getColumnName(Column.Code) == null ? "" : CODE_COLUMN)
            .replace("#code#", seqCodeColumn)
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

    private SeqInfo querySeqInfo(Connection connection, String seqName, boolean hasCode) throws SQLException {
        try (
            PreparedStatement ps = createPs(connection, this.queryCodeTemplate, seqName);
            ResultSet rs = ps.executeQuery()
        ) {
            if (rs.next()) {
                SeqInfo seqInfo = new SeqInfo();
                seqInfo.max = rs.getLong(getColumnName(Column.Max));
                seqInfo.code = hasCode ? rs.getString(getColumnName(Column.Code)) : null;
                return seqInfo;
            }
        }
        throw new IllegalStateException("query result is empty");
    }

    private PreparedStatement createPs(Connection connection, String sql, Object... args) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i = 0; i < args.length; i++) {
            ps.setObject(i + 1, args[i]);
        }
        return ps;
    }
}
