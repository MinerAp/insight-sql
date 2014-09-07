package com.amshulman.insight.sql;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Table;

public final class ForeignKeyCache {

    private static final String ACTORS_QUERY = "SELECT `name`, `id`, `uuid` FROM `actors`";
    private static final String ACTIONS_QUERY = "SELECT `name`, `id` FROM `actions`";
    private static final String MATERIALS_QUERY = "SELECT `namespace`, `name`, `subtype`, `id` FROM `materials`";

    private final Map<String, Integer> actorCache = new HashMap<>();
    private final BiMap<UUID, String> uuidCache = HashBiMap.create();
    private final Map<String, Byte> actionCache = new HashMap<>();
    private final Table<String, Short, Short> materialCache = HashBasedTable.create();

    private final ReadLock readLock;
    private final WriteLock writeLock;

    public ForeignKeyCache(ConnectionPool cp) throws SQLException {
        ReadWriteLock masterLock = new ReentrantReadWriteLock();
        readLock = (ReadLock) masterLock.readLock();
        writeLock = (WriteLock) masterLock.writeLock();

        try (Connection conn = cp.getConnection();

             PreparedStatement actors = conn.prepareStatement(ACTORS_QUERY);
             PreparedStatement actions = conn.prepareStatement(ACTIONS_QUERY);
             PreparedStatement materials = conn.prepareStatement(MATERIALS_QUERY);

             ResultSet actorsRows = actors.executeQuery();
             ResultSet actionsRows = actions.executeQuery();
             ResultSet materialsRows = materials.executeQuery()) {

            while (actorsRows.next()) {
                actorCache.put(actorsRows.getString(1), actorsRows.getInt(2));

                byte[] uuid = actorsRows.getBytes(3);
                if (uuid != null) {
                    ByteBuffer bb = ByteBuffer.wrap(uuid);
                    uuidCache.put(new UUID(bb.getLong(), bb.getLong()), actorsRows.getString(1));
                }
            }

            while (actionsRows.next()) {
                actionCache.put(actionsRows.getString(1), actionsRows.getByte(2));
            }

            while (materialsRows.next()) {
                materialCache.put(getMaterialKey(materialsRows.getString(1), materialsRows.getString(2)), materialsRows.getShort(3), materialsRows.getShort(4));
            }
        }
    }

    public void acquireReadLock() {
        readLock.lock();
    }

    public void acquireWriteLock() {
        writeLock.lock();
    }

    public void releaseReadLock() {
        readLock.unlock();
    }

    public void releaseWriteLock() {
        writeLock.unlock();
    }

    public void upgradeReadLock() {
        readLock.unlock();
        writeLock.lock();
    }

    public void downgradeWriteLock() {
        readLock.lock();
        writeLock.unlock();
    }

    public boolean containsActor(String actor) {
        return actorCache.containsKey(actor);
    }

    public boolean containsUUID(UUID uuid) {
        return uuidCache.containsKey(uuid);
    }

    public boolean containsAction(String action) {
        return actionCache.containsKey(action);
    }

    public boolean containsMaterial(String namespace, String name, short subtype) {
        return materialCache.contains(getMaterialKey(namespace, name), subtype);
    }

    public int getActorId(String actor) {
        return actorCache.get(actor);
    }

    public String getActor(UUID uuid) {
        return uuidCache.get(uuid);
    }

    public byte getActionId(String action) {
        return actionCache.get(action);
    }

    public short getMaterialId(String namespace, String name, short subtype) {
        return materialCache.get(getMaterialKey(namespace, name), subtype);
    }

    public UUID getUUID(String actor) {
        return uuidCache.inverse().get(actor);
    }

    public void addActor(String actor, Integer id) {
        actorCache.put(actor, id);
    }

    public void addUUID(UUID uuid, String actor) {
        uuidCache.put(uuid, actor);
    }

    public void addAction(String action, Byte id) {
        actionCache.put(action, id);
    }

    public void addMaterial(String namespace, String name, short subtype, short id) {
        materialCache.put(getMaterialKey(namespace, name), subtype, id);
    }

    public int removeActor(String actor) {
        return actorCache.remove(actor);
    }

    private static String getMaterialKey(String namespace, String name) {
        return namespace + name;
    }
}
