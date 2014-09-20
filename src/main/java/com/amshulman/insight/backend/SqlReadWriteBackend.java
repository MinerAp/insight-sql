package com.amshulman.insight.backend;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
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
import com.amshulman.insight.row.RowEntry;
import com.amshulman.insight.sql.ConnectionPool;
import com.amshulman.insight.sql.DatabaseDumper;
import com.amshulman.insight.sql.ForeignKeyCache;
import com.amshulman.insight.sql.SqlSelectionQueryBuilder;
import com.amshulman.insight.sql.TableCreator;
import com.amshulman.insight.tbd.RowCache;
import com.amshulman.insight.util.InsightDatabaseConfigurationInfo;
import com.amshulman.insight.util.PlayerUtil;

public class SqlReadWriteBackend implements ReadBackend, WriteBackend {

    private final String databaseName;

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
            TableCreator.createBasicTables(cp);
            keyCache = new ForeignKeyCache(cp);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        databaseName = configurationContext.getDatabaseName();

        logger = configurationContext.getLogger();
        cache = new RowCache(maxCacheSize);
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
    public Set<String> getWorlds() {
        Set<String> worlds = new HashSet<String>(10);
        try (Connection conn = cp.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT REPLACE(`TABLE_NAME`, '_blocks', '') FROM `INFORMATION_SCHEMA`.`TABLES` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` LIKE \"%_blocks%\"");) {
            stmt.setString(1, databaseName);
            stmt.execute();
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                worlds.add(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return Collections.emptySet();
        }

        return worlds;
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

            if (keyCache.containsActor(playerName) && !playerName.equals(keyCache.getActor(uuid))) {
                // The player we knew about has changed their name and someone else has taken their old name
                String newName = PlayerUtil.getCurrentName(keyCache.getUUID(playerName));
                keyCache.releaseReadLock();

                try {
                    registerPlayer(newName, keyCache.getUUID(playerName));
                } finally {
                    keyCache.acquireReadLock();
                }
            }

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

    @Override
    public void registerWorld(String worldName) {
        try {
            TableCreator.createWorldTables(cp, worldName);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] getBytes(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }
}
