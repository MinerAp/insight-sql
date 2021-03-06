package com.amshulman.insight.sql;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import com.amshulman.insight.util.InsightDatabaseConfigurationInfo;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectionPool implements Closeable {

    private static final long INVALID = -1;

    private final HikariDataSource ds;

    public ConnectionPool(InsightDatabaseConfigurationInfo configurationContext) {
        HikariConfig config = new HikariConfig();
        long idleTimeout = INVALID;

        switch (configurationContext.getDatabaseType()) {
            case MYSQL:
                config.setDataSourceClassName(org.mariadb.jdbc.MariaDbDataSource.class.getName());
                idleTimeout = getMySqlIdleTimeout(configurationContext);
                break;
            case POSTGRES:
                config.setDataSourceClassName(org.postgresql.ds.PGSimpleDataSource.class.getName());
                break;
            default:
                throw new IllegalArgumentException();
        }

        config.setInitializationFailTimeout(3000);
        config.addDataSourceProperty("serverName", configurationContext.getDatabaseAddress());
        config.addDataSourceProperty("port", configurationContext.getDatabasePort());
        config.addDataSourceProperty("databaseName", configurationContext.getDatabaseName());

        config.setUsername(configurationContext.getDatabaseUsername());
        config.setPassword(configurationContext.getDatabasePassword());
        config.setPoolName("Insight");
        config.addDataSourceProperty("properties", "rewriteBatchedStatements=true&useFractionalSeconds=true&useUnicode=true&characterEncoding=utf-8");
        if (idleTimeout != INVALID) {
            try {
                config.setIdleTimeout((long) (TimeUnit.SECONDS.toMillis(idleTimeout) * 0.9));
            } catch (SecurityException e) {
                e.printStackTrace();
            }
        }

        ds = new HikariDataSource(config);

        // System.out.println("[DEBUG] Using conncection idle timeout of " + ds.getIdleTimeout());
    }

    /**
     * Returns a free connection.
     *
     * @return Connection handle.
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    @Override
    public void close() {
        ds.close();
    }

    private static long getMySqlIdleTimeout(InsightDatabaseConfigurationInfo configurationContext) {
        try (
             Connection conn = DriverManager.getConnection("jdbc:mysql://" +
                     configurationContext.getDatabaseAddress() +
                     ":" + configurationContext.getDatabasePort() + "/",
                                                           configurationContext.getDatabaseUsername(),
                                                           configurationContext.getDatabasePassword());
             Statement stmt = conn.createStatement();) {

            stmt.execute("SHOW VARIABLES LIKE \"wait_timeout\"");
            ResultSet rs = stmt.getResultSet();
            rs.next();
            long waitTimeout = rs.getLong("Value");

            assert (!rs.next());
            stmt.close();
            conn.close();

            return waitTimeout;
        } catch (SQLException | AssertionError e) {}

        return INVALID;
    }
}
