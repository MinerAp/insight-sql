package com.amshulman.insight.backend;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.amshulman.insight.query.QueryParameterBuilder;
import com.amshulman.insight.query.QueryParameters;
import com.amshulman.insight.results.InsightResultSet;
import com.amshulman.insight.results.InsightSqlResultSet;
import com.amshulman.insight.row.DatabaseDumper;
import com.amshulman.insight.row.RowEntry;
import com.amshulman.insight.sql.ConnectionPool;
import com.amshulman.insight.sql.ForeignKeyCache;
import com.amshulman.insight.sql.ScriptRunner;
import com.amshulman.insight.sql.SqlSelectionQueryBuilder;
import com.amshulman.insight.tbd.RowCache;
import com.amshulman.insight.util.InsightDatabaseConfigurationInfo;

public class SqlReadWriteBackend implements ReadBackend, WriteBackend {

    private int maxCacheSize = 100;
    private int targetCacheSize = 75;
    private RowCache cache;

    private final Logger logger;

    private final ForeignKeyCache keyCache;
    private final ConnectionPool cp;
    private final ExecutorService writeThreads = new ThreadPoolExecutor(2, 10, 1, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100));

    public SqlReadWriteBackend(InsightDatabaseConfigurationInfo configurationContext) {
        try {
            cp = new ConnectionPool(configurationContext);

            if (BackendType.MYSQL.equals(configurationContext.getDatabaseType())) {
                runScript(getClass().getResourceAsStream("/mysql"));
            }

            keyCache = new ForeignKeyCache(cp);
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }

        logger = configurationContext.getLogger();
        cache = new RowCache(maxCacheSize);
    }

    private void runScript(InputStream in) throws SQLException, IOException {
        try (Connection conn = cp.getConnection()) {
            ScriptRunner.runScript(conn, new BufferedReader(new InputStreamReader(in, "UTF-8")), true, true);
        }
    }

    @Override
    public void submit(RowEntry data) {
        cache.add(data);

        if (cache.isFull()) {
            flushCache(true);
        }
    }

    @Override
    public InsightResultSet submit(QueryParameters params) {

        flushCache(false);
        InsightResultSet results;

        try (Connection c = cp.getConnection();
             PreparedStatement stmt = SqlSelectionQueryBuilder.build(params).getPreparedStatement(c)) {
            stmt.execute();
            results = new InsightSqlResultSet(stmt.getResultSet(), params);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        return results;
    }

    @Override
    public void suggestFlush() {
        if (cache.getSize() > targetCacheSize) {
            flushCache(true);
        }
    }

    private void flushCache(boolean async) {
        if (cache.isDirty()) {
            if (async) {
                writeThreads.execute(new DatabaseDumper(cp, cache, keyCache));
            } else {
                new DatabaseDumper(cp, cache, keyCache).run();
            }

            cache = new RowCache(maxCacheSize);
        }
    }

    @Override
    public QueryParameterBuilder newQueryBuilder() {
        return new QueryParameterBuilder();
    }

    @Override
    public void close() {
        flushCache(false);
        writeThreads.shutdown();
        boolean cleanShutdown;
        try {
            cleanShutdown = writeThreads.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            cleanShutdown = false;
        }

        if (!cleanShutdown) {
            logger.severe("Problem shutting down cleanly"); // TODO
        }

        cp.close();
    }

    @Override
    public void registerPlayer(String playerName, UUID uuid) {
        boolean insert = true; // Assume we haven't seen the player before

        keyCache.acquireReadLock();
        try {
            if (keyCache.containsUUID(uuid)) {
                if (keyCache.getActor(uuid).equals(playerName)) {
                    return; // Information is correct
                } else {
                    insert = false; // We have seen this UUID before but with a different name
                }
            }
        } finally {
            keyCache.releaseReadLock();
        }

        keyCache.acquireWriteLock();
        try {
            if (insert) {
                if (!keyCache.containsUUID(uuid)) {
                    try (Connection conn = cp.getConnection();
                         PreparedStatement insertActor = conn.prepareStatement("INSERT INTO `actors`(`name`, `uuid`) VALUES (?, ?)", PreparedStatement.RETURN_GENERATED_KEYS);) {
                        insertActor.setString(1, playerName);
                        insertActor.setBytes(2, getBytes(uuid));
                        insertActor.executeUpdate();

                        ResultSet rs = insertActor.getGeneratedKeys();
                        rs.next();
                        keyCache.addActor(playerName, rs.getInt(1));
                    }

                    keyCache.addUUID(uuid, playerName);
                }
            } else {
                String oldActor = keyCache.getActor(uuid);
                if (!oldActor.equals(playerName)) {
                    try (Connection conn = cp.getConnection();
                         PreparedStatement updateActor = conn.prepareStatement("UPDATE `actors` SET `name` = ? WHERE `uuid` = ? ");) {
                        updateActor.setString(1, playerName);
                        updateActor.setBytes(2, getBytes(uuid));
                        updateActor.executeUpdate();
                    }

                    keyCache.addActor(playerName, keyCache.removeActor(oldActor));
                    keyCache.addUUID(uuid, playerName);
                }
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            keyCache.releaseWriteLock();
        }
    }

    private static byte[] getBytes(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }
}
