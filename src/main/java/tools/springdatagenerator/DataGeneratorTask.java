package tools.springdatagenerator;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.springframework.jdbc.core.JdbcTemplate;

import lombok.extern.slf4j.Slf4j;
import tools.springdatagenerator.beans.RuntimeConfig;

@Slf4j
public class DataGeneratorTask implements Runnable {
    private static final int MAX_PRECISION_MINUS_SCALE = 6;

    private final JdbcTemplate jdbcTemplate;
    private final RuntimeConfig runtimeConfig;
    private final String table;
    private final long startHintValue;
    private final Map<String, String> values;
    private final Map<String, Set<String>> table2Pks = new HashMap<String, Set<String>>();

    private volatile boolean jvmStopped = false;
    private Random random = new Random();

    public DataGeneratorTask(JdbcTemplate jdbcTemplate, RuntimeConfig runtimeConfig,
            String table, long startHintValue,
            Map<String, String> values, boolean jvmStopped) {
        this.jdbcTemplate = jdbcTemplate;
        this.runtimeConfig = runtimeConfig;
        this.table = table;
        this.startHintValue = startHintValue;
        this.values = values;
        this.jvmStopped = jvmStopped;
    }

    @Override
    public void run() {
        Connection conn = null;
        InsertStatement insert = null;
        DeleteStatement delete = null;
        long totalRowsInserted = 0; // total inserted per thread
        long totalRowsDeleted = 0; // total deleted per thread
        long totalRowsUpdated = 0; // total updated per thread
        int tryRound = 0;
        long startTime = System.currentTimeMillis();
        try {
            conn = jdbcTemplate.getDataSource().getConnection();
            conn.setAutoCommit(false);
            if (runtimeConfig.needTruncate) {
                totalRowsDeleted += oneTimeDeleteAll(conn);
            }

            queryPrimaryKeys(conn);
            insert = createInsertStatement(conn);
            delete = createDeleteStatement(conn);

            long i = startHintValue;
            long currentRun = 1;
            long currentRowsInserted = 0; // roughly per table (max inserted of all tables per round)
            boolean isDeleteOnlyFinished = true;
            boolean isInsertFinished = true;
            boolean isUpdateFinished = true;
            do {
                log.info("--- [THR-" + Thread.currentThread().getId() + "] Round " + currentRun);

                // Delete
                if (runtimeConfig.maxInsertCountPerThread == 0
                        && runtimeConfig.maxUpdateCountPerThread == 0
                        && runtimeConfig.deleteThreshold > 0) {
                    // only delete
                    int deleted = deleteRandomly(delete);
                    conn.commit();
                    totalRowsDeleted += deleted;
                    isDeleteOnlyFinished = (deleted == 0);
                } else if (runtimeConfig.deleteThreshold > 0
                        && currentRowsInserted > runtimeConfig.deleteThreshold) {
                    totalRowsDeleted += deleteRandomly(delete);
                    // commit for each delete as delete is very slow on this Oracle which may never finish on interrupt
                    conn.commit();
                    currentRowsInserted = 0;
                }

                // Insert
                if (runtimeConfig.maxInsertCountPerThread > 0
                        && totalRowsInserted < runtimeConfig.maxInsertCountPerThread
                        && currentRowsInserted <= runtimeConfig.maxInsertCountPerThread) {
                    int rowsInserted = insert(
                            insert,
                            i,
                            runtimeConfig.maxInsertCountPerThread - totalRowsInserted);
                    totalRowsInserted += rowsInserted;
                    conn.commit();

                    i += rowsInserted;
                    currentRowsInserted += rowsInserted;
                }
                isInsertFinished = (totalRowsInserted >= runtimeConfig.maxInsertCountPerThread);

                // Update
                if (runtimeConfig.maxUpdateCountPerThread > 0
                        && totalRowsUpdated < runtimeConfig.maxUpdateCountPerThread) {
                    if (totalRowsInserted == 0) {
                        throw new SQLException("Limitation: can not do UPDATE without INSERT!");
                    }
                    totalRowsUpdated += updateRandomly(conn, i - startHintValue);
                    ++tryRound;
                    conn.commit();
                }
                isUpdateFinished = (totalRowsUpdated >= runtimeConfig.maxUpdateCountPerThread)
                        || (tryRound >= runtimeConfig.maxTryRound);

                ++currentRun;
            } while (!jvmStopped && (!isDeleteOnlyFinished || !isInsertFinished || !isUpdateFinished));
        } catch (SQLException ex) {
            log.error(ex.getMessage(), ex);
            try {
                conn.rollback();
            } catch (SQLException e) {
            }
        } finally {
            try {
                insert.ps.close();
            } catch (SQLException e) {
                insert.ps = null;
            }
            try {
                delete.ps.close();
            } catch (SQLException e) {
                delete.ps = null;
            }
            try {
                conn.close();
            } catch (SQLException e) {
                conn = null;
            }
        }
        long endTime = System.currentTimeMillis();
        log.info("--- [THR-" + Thread.currentThread().getId() + "] Total rows inserted: " + totalRowsInserted
                + ", total rows deleted: " + totalRowsDeleted
                + ", total rows updated: " + totalRowsUpdated
                + ", total time: " + (endTime - startTime)
                + ", table: [" + table + "]");
        log.info("Data generator exit.");
    }

    private void queryPrimaryKeys(Connection conn) throws SQLException {
        String[] splits = table.split("\\.");
        if (splits.length != 2) {
            String errorMessage = "Only support <table_owner>.<table_name> without special characters!";
            log.error(errorMessage);
            throw new SQLException(errorMessage);
        }
        java.sql.DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getPrimaryKeys(null, splits[0], splits[1]);
        Set<String> pks = new HashSet<String>();
        while (rs.next()) {
            pks.add(rs.getString("COLUMN_NAME"));
            }
        if (!pks.isEmpty()) {
            table2Pks.put(table, pks);
        }
    }

    protected static class InsertStatement {
        public String tablename;
        public PreparedStatement ps = null;
        public int[] types = null;
        public int[] lengths = null;
        public int[] scales = null;
        public String[] columnNames = null;
        public InsertStatement(String tablename, PreparedStatement ps, int[] columnTypes, int[] columnLengths, 
        		int[] columnScales, String[] columnNames)
        {
            this.tablename = tablename;
            this.ps = ps;
            this.types = columnTypes;
            this.lengths = columnLengths;
            this.scales = columnScales;
            this.columnNames = columnNames;
        }
    }

    protected static class DeleteStatement {
        public String tablename;
        public PreparedStatement ps = null;
        public DeleteStatement(String tablename, PreparedStatement ps)
        {
            this.tablename = tablename;
            this.ps = ps;
        }
    }

    private long oneTimeDeleteAll(Connection conn) throws SQLException {
        long ret = 0;
        DeleteStatement delete = createDeleteAllStatement(conn, table);
        try {
            ret += deleteAll(delete);
            conn.commit();
        } finally {
            delete.ps.close();
        }
        return ret;
    }

    private InsertStatement createInsertStatement(Connection conn) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder sb = new StringBuilder();
        int[] columnTypes = null;
        int[] columnLengths = null;
        int[] columnScales = null;
        String[] columnNames = null;
        try {
            sb.append("INSERT INTO ").append(table).append(" VALUES(");
            ps = conn.prepareStatement("select * from " + table + " where 1=0");
            rs = ps.executeQuery();
            ResultSetMetaData rsmd = ps.getMetaData();
            int colCount = rsmd.getColumnCount();
            columnTypes = new int[colCount];
            columnLengths = new int[colCount];
            columnScales = new int[colCount];
            columnNames = new String[colCount];
            for (int i = 0; i < colCount; ++i) {
                columnTypes[i] = rsmd.getColumnType(i + 1);
                columnLengths[i] = rsmd.getPrecision(i + 1);
                columnScales[i] = rsmd.getScale(i + 1);
                columnNames[i] = rsmd.getColumnName(i + 1);
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append("?");
            }
            sb.append(")");
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        } finally {
            rs.close();
            ps.close();
        }
        return new InsertStatement(table, conn.prepareStatement(sb.toString()),
                columnTypes, columnLengths, columnScales, columnNames);
    }

    private DeleteStatement createDeleteAllStatement(Connection conn, String table ) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(table);
        return new DeleteStatement(table, conn.prepareStatement(sb.toString()));
    }

    private DeleteStatement createDeleteStatement(Connection conn) throws SQLException {
    	PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder sb = new StringBuilder();
        StringBuilder pks = new StringBuilder();
        try {
            if (table2Pks.containsKey(table)) {
                boolean isFirst = true;
                for (String pk : table2Pks.get(table)) {
                    if (isFirst) {
                        isFirst = false;
                    } else {
                        pks.append("||");
                    }
                    pks.append("\"").append(pk).append("\"");
                }
            } else {
                ps = conn.prepareStatement("select * from " + table + " where 1=0");
                rs = ps.executeQuery();
                ResultSetMetaData rsmd = rs.getMetaData();
                pks.append(rsmd.getColumnName(1));
            }

            sb.append("DELETE FROM ").append(table)
              .append(" WHERE ").append(pks.toString())
              .append(" IN (SELECT TOP ? ").append(pks.toString())
              .append(" FROM ").append(table).append(")");
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        } finally {
            rs.close();
            ps.close();
        }
        return new DeleteStatement(table, conn.prepareStatement(sb.toString()));
    }

    private int insert(InsertStatement insert, long hintValue, long rest) throws SQLException {
        final int maxRowsToGenerate = runtimeConfig.maxTransactionSize;
        int count = 0;
        PreparedStatement ps = insert.ps;
        int rowsToInsert = random.nextInt(maxRowsToGenerate) + 1;
        rowsToInsert = (int) (rowsToInsert > rest ? rest : rowsToInsert);
        for (int j = 0; j < rowsToInsert; ++j) {
            for (int i = 0; i < insert.types.length; ++i) {
                int type = insert.types[i];
                String columnName = insert.columnNames[i];
                switch (type) {
                case Types.BIGINT:
                    if (values.containsKey(columnName)) {
                        ps.setLong(i+1, Long.valueOf(values.get(columnName)));
                    } else {
                        ps.setLong(i+1, hintValue + count);
                    }
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                    if (values.containsKey(columnName)) {
                        ps.setString(i+1, values.get(columnName));
                    } else {
                        ps.setString(i+1, genString(hintValue + count, insert.lengths[i]));
                    }
                    break;
                case Types.DATE:
                    if (values.containsKey(columnName)) {
                        ps.setDate(i+1, Date.valueOf(values.get(columnName)));
                    } else {
                        ps.setDate(i+1, new Date(System.currentTimeMillis()));
                    }
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    if (values.containsKey(columnName)) {
                        ps.setBigDecimal(i+1, BigDecimal.valueOf(Long.valueOf(values.get(columnName))));
                    } else if (insert.lengths[i] - insert.scales[i] < MAX_PRECISION_MINUS_SCALE) {
                        ps.setBigDecimal(i+1, new BigDecimal(0));
                    } else {
                        ps.setBigDecimal(i+1, new BigDecimal(hintValue + count));
                    }
                    break;
                case Types.DOUBLE:
                    if (values.containsKey(columnName)) {
                        ps.setDouble(i+1, Double.valueOf(values.get(columnName)));
                    } else {
                        ps.setDouble(i+1, hintValue + count);
                    }
                    break;
                case Types.FLOAT:
                case Types.REAL:
                    if (values.containsKey(columnName)) {
                        ps.setFloat(i+1, Float.valueOf(values.get(columnName)));
                    } else {
                        ps.setFloat(i+1, hintValue + count);
                    }
                    break;
                case Types.SMALLINT:
                    if (values.containsKey(columnName)) {
                        ps.setShort(i+1, Short.valueOf(values.get(columnName)));
                    } else {
                        ps.setShort(i+1, (short) (hintValue + count));
                    }
                    break;
                case Types.INTEGER:
                    if (values.containsKey(columnName)) {
                        ps.setInt(i+1, Integer.valueOf(values.get(columnName)));
                    } else {
                        ps.setInt(i+1, (int) hintValue + count);
                    }
                    break;
                case Types.NCHAR:
                case Types.NVARCHAR:
                    if (values.containsKey(columnName)) {
                        ps.setNString(i+1, values.get(columnName));
                    } else {
                        ps.setNString(i+1, genString(hintValue + count, insert.lengths[i]));
                    }
                    break;
                case Types.TIME:
                case Types.TIME_WITH_TIMEZONE:
                    if (values.containsKey(columnName)) {
                        ps.setTime(i+1, Time.valueOf(values.get(columnName)));
                    } else {
                        ps.setTime(i+1, new Time(System.currentTimeMillis()));
                    }
                    break;
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    if (values.containsKey(columnName)) {
                        ps.setTimestamp(i+1, Timestamp.valueOf(values.get(columnName)));
                    } else {
                        ps.setTimestamp(i+1, new Timestamp(System.currentTimeMillis()));
                    }
                    break;
                default:
                    ps.setNull(i+1, type);
                }
            }
            ps.addBatch();
            ++count;
        }
        try {
            ps.executeBatch();
            log.info(new StringBuilder().append("Inserted to ").append(insert.tablename)
                    .append("; hint = ").append(hintValue - startHintValue)
                    .append("; rows = ").append(count).toString());
        } catch (SQLException e) {
            if (e.getErrorCode() == 301 || e.getMessage().toLowerCase().contains("unique constraint")) {
                // unique constraint violated
                log.warn("Ignore unique constraint violated exception and keep running...");
            } else {
                throw e;
            }
        }
        return count;
    }

    private int deleteAll(DeleteStatement delete) throws SQLException {
        int deleteCount = delete.ps.executeUpdate();
        log.info(new StringBuilder("Deleted from ").append(delete.tablename)
                .append("; rows = ").append(deleteCount).toString());
        return deleteCount;
    }

    private int deleteRandomly(DeleteStatement delete) throws SQLException {
        delete.ps.setInt(1, random.nextInt((int) runtimeConfig.deleteThreshold)+1);
        int deleteCount = delete.ps.executeUpdate();
        log.info(new StringBuilder("Deleted from ").append(delete.tablename)
                .append("; rows = ").append(deleteCount).toString());
        return deleteCount;
  }

    private long updateRandomly(Connection conn, long hintValue) {
        long updatedCount = 0L;
        PreparedStatement stmt = null;
        ResultSet stmtRs = null;
        PreparedStatement ps = null;
        StringBuilder sb = new StringBuilder();
        String[] columnNames = null;
        int[] columnTypes = null;
        int[] columnLengths = null;
        int[] columnScales = null;
        try {
            stmt = conn.prepareStatement("select * from " + table + " where 1=0");
            stmtRs = stmt.executeQuery();
            ResultSetMetaData rsmd = stmtRs.getMetaData();
            int colCount = rsmd.getColumnCount();
            columnNames = new String[colCount];
            columnTypes = new int[colCount];
            columnLengths = new int[colCount];
            columnScales = new int[colCount];
            for (int i = 0; i < colCount; ++i) {
                columnNames[i] = rsmd.getColumnName(i + 1);
                columnTypes[i] = rsmd.getColumnType(i + 1);
                columnLengths[i] = rsmd.getPrecision(i + 1);
                columnScales[i] = rsmd.getScale(i + 1);
            }
            int randomIndex = random.nextInt(colCount);
            int maxTriedRound = runtimeConfig.maxTryRound;
            while (table2Pks.containsKey(table)
                    && table2Pks.get(table).contains(columnNames[randomIndex])
                    && values.containsKey(columnNames[randomIndex])
                    && maxTriedRound > 0) {
                randomIndex = random.nextInt(colCount);
                --maxTriedRound;
            }
            if (maxTriedRound == 0) {
                return 0;
            }

            int index = random.nextInt(colCount);
            int type = columnTypes[randomIndex];
            String colName = columnNames[randomIndex];
            if (values.containsKey(colName)) {
                return 0;
            }
            sb.append("UPDATE ").append(table).append(" SET \"").append(colName).append("\" = ? ")
                .append("WHERE \"").append(columnNames[index]).append("\" BETWEEN ? AND ?");
            ps = conn.prepareStatement(sb.toString());
            long randomLong = 0L;
            for (int paramIndex = 0; paramIndex < 3; ++paramIndex) {
                if (paramIndex == 0) {
                    type = columnTypes[randomIndex];
                } else {
                    type = columnTypes[index];
                }

                if (paramIndex == 2) {
                    randomLong = randomLong + random.nextInt(runtimeConfig.maxTransactionSize);
                } else {
                    randomLong = random.nextInt((int) hintValue) + 1 + startHintValue;
                }

                switch (type) {
                case Types.BIGINT:
                    ps.setLong(paramIndex + 1, randomLong);
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                    ps.setString(paramIndex + 1, genString(randomLong, columnLengths[randomIndex]));
                    break;
                case Types.DATE:
                    ps.setDate(paramIndex + 1, new Date(System.currentTimeMillis()));
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    if (columnLengths[randomIndex] - columnScales[randomIndex] < MAX_PRECISION_MINUS_SCALE) {
                        ps.setBigDecimal(paramIndex + 1, new BigDecimal(0));
                    } else {
                        ps.setBigDecimal(paramIndex + 1, new BigDecimal(randomLong));
                    }
                    break;
                case Types.DOUBLE:
                    ps.setDouble(paramIndex + 1, randomLong);
                    break;
                case Types.FLOAT:
                case Types.REAL:
                    ps.setFloat(paramIndex + 1, randomLong);
                    break;
                case Types.SMALLINT:
                	ps.setShort(paramIndex+1, (short)randomLong);
                    break;
                case Types.INTEGER:
                    ps.setInt(paramIndex + 1, (int)randomLong);
                    break;
                case Types.NCHAR:
                case Types.NVARCHAR:
                    ps.setNString(paramIndex + 1, genString(randomLong, columnLengths[randomIndex]));
                    break;
                case Types.TIME:
                case Types.TIME_WITH_TIMEZONE:
                    ps.setTime(paramIndex + 1, new Time(System.currentTimeMillis()));
                    break;
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    ps.setTimestamp(paramIndex + 1, new Timestamp(System.currentTimeMillis()));
                    break;
                default:
                    ps.setNull(paramIndex + 1, type);
                }
            }
            updatedCount =  ps.executeUpdate();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        } finally {
            try {
                if (stmtRs != null)
                    stmtRs.close();
            } catch (SQLException e) {
                stmtRs = null;
            }
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException e) {
                stmt = null;
            }
            try {
                if (ps != null)
                    ps.close();
            } catch (SQLException e) {
                ps = null;
            }
        }
        log.info(new StringBuilder("Updated ").append(table)
                .append("; rows = ").append(updatedCount).toString());
        return updatedCount;
    }

    private String genString(long hintValue, int len) {
        StringBuilder sb = new StringBuilder();
        if (len == 1) {
            sb.append('Y'); // usually constraint 'Y/N'
        } else {
            sb.append("B");
            sb.append(hintValue); //  random.nextInt((int) (hintValue - startHintValue + 1)) + startHintValue
            sb.append("-");
            for (int i = sb.length(); i < len; ++i) {
                sb.append((char)('B' + random.nextInt(26)));
            }
        }
        if (sb.length() > len) {
            return sb.substring(0, len);
        }
        return sb.toString();
    }
}
