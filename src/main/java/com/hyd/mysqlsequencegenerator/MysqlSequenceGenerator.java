package com.hyd.mysqlsequencegenerator;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Optional.ofNullable;

public class MysqlSequenceGenerator {

    public static final String DEFAULT_UPDATE = "update #table# " +
        "set #seqvalue# = last_insert_id(" +
        "  if(#seqvalue# + #step# > #max#, 0, #seqvalue# + #step#)" +
        ") where #seqname# = ?";

    public static final String DEFAULT_QUERY = "SELECT " +
        "last_insert_id(), last_insert_id() + #step# FROM #table#";

    public static final String DEFAULT_TABLE_NAME = "t_sequence";

    public static final String DEFAULT_SEQ_NAME_COLUMN = "name";

    public static final String DEFAULT_SEQ_VALUE_COLUMN = "value";

    public static final String DEFAULT_SEQ_STEP_COLUMN = "step";

    public static final String DEFAULT_SEQ_MAX_COLUMN = "max";

    ////////////////////////////////////////////////////////////

    @FunctionalInterface
    interface F<A, B> {

        B f(A a) throws SQLException;
    }

    class Counter {

        String sequenceName;

        AtomicLong max = new AtomicLong();

        AtomicLong value = new AtomicLong();

        public Counter(String sequenceName, long min, long max) {
            this.sequenceName = sequenceName;
            this.value.set(min);
            this.max.set(max);
        }

        public long next() {
            return value.updateAndGet(l -> {
                if (l + 1 >= max.get()) {
                    long[] minMax = refreshCounter();
                    max.set(minMax[1]);
                    return minMax[0];
                } else {
                    return l + 1;
                }
            });
        }

        private long[] refreshCounter() {
            try {
                return withConnection(connection -> {
                    executeUpdate(connection, sequenceName);
                    return MysqlSequenceGenerator.this.refreshCounter(connection);
                });
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    ////////////////////////////////////////////////////////////

    private DataSource dataSource;

    private boolean autoCloseConnection;

    private String updateTemplate;

    private String queryTemplate;

    private Map<String, Counter> counters = new ConcurrentHashMap<>();

    private BiConsumer<Long, Long> onSequenceUpdate;

    public MysqlSequenceGenerator(
        DataSource dataSource, boolean autoCloseConnection,
        String tableName, String seqValueColumn, String seqNameColumn, String seqStepColumn, String seqMaxColumn
    ) {

        tableName = ofNullable(tableName).orElse(DEFAULT_TABLE_NAME);
        seqNameColumn = ofNullable(seqNameColumn).orElse(DEFAULT_SEQ_NAME_COLUMN);
        seqValueColumn = ofNullable(seqValueColumn).orElse(DEFAULT_SEQ_VALUE_COLUMN);
        seqStepColumn = ofNullable(seqStepColumn).orElse(DEFAULT_SEQ_STEP_COLUMN);
        seqMaxColumn = ofNullable(seqMaxColumn).orElse(DEFAULT_SEQ_MAX_COLUMN);

        this.dataSource = dataSource;
        this.autoCloseConnection = autoCloseConnection;

        this.updateTemplate = DEFAULT_UPDATE
            .replace("#table#", tableName)
            .replace("#seqvalue#", seqValueColumn)
            .replace("#step#", seqStepColumn)
            .replace("#seqname#", seqNameColumn)
            .replace("#max#", seqMaxColumn);

        this.queryTemplate = DEFAULT_QUERY
            .replace("#table#", tableName)
            .replace("#step#", seqStepColumn);
    }


    public DataSource getDataSource() {
        return dataSource;
    }

    public boolean isAutoCloseConnection() {
        return autoCloseConnection;
    }

    public String getUpdateTemplate() {
        return updateTemplate;
    }

    public String getQueryTemplate() {
        return queryTemplate;
    }

    public void setOnSequenceUpdate(BiConsumer<Long, Long> onSequenceUpdate) {
        this.onSequenceUpdate = onSequenceUpdate;
    }

    ////////////////////////////////////////////////////////////

    private DataSource dataSource() {
        return ofNullable(this.dataSource)
            .orElseThrow(() -> new IllegalStateException("DataSource is null"));
    }

    public Long nextLong(String sequenceName) throws SQLException {
        Counter counter = counters.computeIfAbsent(sequenceName, s -> new Counter(s, 0, 0));
        return counter.next();
    }

    private <T> T withConnection(F<Connection, T> f) throws SQLException {
        Connection connection = dataSource().getConnection();
        try {
            return f.f(connection);
        } finally {
            if (this.autoCloseConnection) {
                connection.close();
            }
        }
    }

    private long[] refreshCounter(Connection connection) throws SQLException {
        try (
            PreparedStatement ps = connection.prepareStatement(this.queryTemplate);
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

    private void executeUpdate(Connection connection, String sequenceName) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(this.updateTemplate)) {
            ps.setString(1, sequenceName);
            ps.executeUpdate();
        }
    }
}
