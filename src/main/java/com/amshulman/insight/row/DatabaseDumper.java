package com.amshulman.insight.row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import lombok.RequiredArgsConstructor;

import com.amshulman.insight.serialization.BlockMetadata;
import com.amshulman.insight.serialization.ItemMetadata;
import com.amshulman.insight.serialization.StorageMetadata;
import com.amshulman.insight.sql.ConnectionPool;
import com.amshulman.insight.sql.ForeignKeyCache;
import com.amshulman.insight.tbd.RowCache;
import com.amshulman.insight.types.InsightMaterial;
import com.amshulman.insight.types.MaterialCompat;
import com.amshulman.insight.util.SerializationUtil;

@RequiredArgsConstructor
public final class DatabaseDumper implements Runnable {

    private final ConnectionPool cp;
    private final RowCache rowCache;
    private final ForeignKeyCache keyCache;

    @Override
    public void run() {
        if (!rowCache.isDirty()) {
            return;
        }

        keyCache.acquireReadLock();
        try {
            // Ensure we know the row ids for all materials, actions, and actors
            // Only NPCs actors will be caught here, players are handled at login
            for (RowEntry row : rowCache) {
                if (row instanceof BlockRowEntry) {
                    InsightMaterial m = MaterialCompat.getInsightMaterial(((BlockRowEntry) row).block);
                    checkMaterial(m.getNamespace(), m.getName(), m.getSubtype());
                } else if (row instanceof ItemRowEntry) {
                    ItemRowEntry itemRow = (ItemRowEntry) row;
                    InsightMaterial m = MaterialCompat.getInsightMaterial(itemRow.itemType, itemRow.damage);
                    checkMaterial(m.getNamespace(), m.getName(), m.getSubtype());
                } else if (row instanceof EntityRowEntry) {
                    checkActor(((EntityRowEntry) row).actee);
                } else {
                    continue;
                }

                checkActor(row.actor);
                checkAction(row.action);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            keyCache.releaseReadLock();
        }

        keyCache.acquireReadLock();
        try (Connection conn = cp.getConnection();
             PreparedStatement insertBlock = conn.prepareStatement("INSERT INTO `world_blocks`(`datetime`, `actionid`, `actorid`, `x`, `y`, `z`, `blockid`, `metadata`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
             PreparedStatement insertItem = conn.prepareStatement("INSERT INTO `world_items`(`datetime`, `actionid`, `actorid`, `x`, `y`, `z`, `itemid`, `metadata`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
             PreparedStatement insertEntity = conn.prepareStatement("INSERT INTO `world_entities`(`datetime`, `actionid`, `actorid`, `x`, `y`, `z`, `acteeid`, `metadata`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");) {
            conn.setAutoCommit(false);

            for (RowEntry row : rowCache) {
                if (row instanceof BlockRowEntry) {
                    setBasicParameters(insertBlock, row);

                    BlockRowEntry blockRow = (BlockRowEntry) row;
                    InsightMaterial m = MaterialCompat.getInsightMaterial(blockRow.block);
                    insertBlock.setShort(7, keyCache.getMaterialId(m.getNamespace(), m.getName(), m.getSubtype()));

                    if (blockRow.metadata == null && blockRow.previousBlock == null) {
                        insertBlock.setNull(8, java.sql.Types.VARBINARY);
                    } else {
                        insertBlock.setBytes(8, SerializationUtil.serializeMetadata(new BlockMetadata(blockRow.metadata, blockRow.previousBlock))); // blockRow.metadata
                    }

                    insertBlock.addBatch();
                } else if (row instanceof ItemRowEntry) {
                    setBasicParameters(insertItem, row);

                    ItemRowEntry itemRow = (ItemRowEntry) row;
                    InsightMaterial m = MaterialCompat.getInsightMaterial(itemRow.itemType, itemRow.damage);
                    insertItem.setShort(7, keyCache.getMaterialId(m.getNamespace(), m.getName(), m.getSubtype()));

                    StorageMetadata meta = null;
                    if (itemRow.metadata.serialize().size() > 1) {
                        meta = new ItemMetadata(itemRow.metadata, itemRow.quantity);
                    } else if (itemRow.quantity > 1) {
                        meta = new ItemMetadata(null, itemRow.quantity);
                    }

                    if (meta != null) {
                        insertItem.setBytes(8, SerializationUtil.serializeMetadata(meta));
                    } else {
                        insertItem.setNull(8, java.sql.Types.VARBINARY);
                    }

                    insertItem.addBatch();
                } else if (row instanceof EntityRowEntry) {
                    setBasicParameters(insertEntity, row);

                    EntityRowEntry entityRow = (EntityRowEntry) row;
                    insertEntity.setInt(7, keyCache.getActorId(entityRow.actee));
                    insertEntity.setNull(8, java.sql.Types.VARBINARY);

                    insertEntity.addBatch();
                }
            }

            insertBlock.executeBatch();
            insertItem.executeBatch();
            insertEntity.executeBatch();

            conn.setAutoCommit(true);
            rowCache.markClean();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            keyCache.releaseReadLock();
        }
    }

    private void setBasicParameters(PreparedStatement statement, RowEntry row) throws SQLException {
        statement.setTimestamp(1, new Timestamp(row.datetime));
        statement.setByte(2, keyCache.getActionId(row.action));
        statement.setInt(3, keyCache.getActorId(row.actor));
        statement.setInt(4, row.x);
        statement.setShort(5, row.y > Short.MAX_VALUE ? Short.MAX_VALUE : (row.y < Short.MIN_VALUE ? Short.MIN_VALUE : (short) row.y));
        statement.setInt(6, row.z);
    }

    private void checkActor(String actorName) throws SQLException {
        if (!keyCache.containsActor(actorName)) {
            keyCache.upgradeReadLock();
            try {
                if (!keyCache.containsActor(actorName)) {
                    try (Connection conn = cp.getConnection();
                         PreparedStatement insertActor = conn.prepareStatement("INSERT INTO `actors`(`name`) VALUES (?)", PreparedStatement.RETURN_GENERATED_KEYS);) {
                        insertActor.setString(1, actorName);
                        insertActor.executeUpdate();

                        ResultSet rs = insertActor.getGeneratedKeys();
                        rs.next();
                        keyCache.addActor(actorName, rs.getInt(1));
                    }
                }
            } finally {
                keyCache.downgradeWriteLock();
            }
        }
    }

    private void checkAction(String actionName) throws SQLException {
        if (!keyCache.containsAction(actionName)) {
            keyCache.upgradeReadLock();
            try {
                if (!keyCache.containsAction(actionName)) {
                    try (Connection conn = cp.getConnection();
                         PreparedStatement insertAction = conn.prepareStatement("INSERT INTO `actions`(`name`) VALUES (?)", PreparedStatement.RETURN_GENERATED_KEYS);) {
                        insertAction.setString(1, actionName);
                        insertAction.executeUpdate();

                        ResultSet rs = insertAction.getGeneratedKeys();
                        rs.next();
                        keyCache.addAction(actionName, rs.getByte(1));
                    }
                }
            } finally {
                keyCache.downgradeWriteLock();
            }
        }
    }

    private void checkMaterial(String namespace, String materialName, short subtype) throws SQLException {
        if (!keyCache.containsMaterial(namespace, materialName, subtype)) {
            keyCache.upgradeReadLock();
            try {
                if (!keyCache.containsMaterial(namespace, materialName, subtype)) {
                    try (Connection conn = cp.getConnection();
                         PreparedStatement insertMaterial = conn.prepareStatement("INSERT INTO `materials`(`namespace`, `name`, `subtype`) VALUES (?, ?, ?)", PreparedStatement.RETURN_GENERATED_KEYS);) {
                        insertMaterial.setString(1, namespace);
                        insertMaterial.setString(2, materialName);
                        insertMaterial.setShort(3, subtype);
                        insertMaterial.executeUpdate();

                        ResultSet rs = insertMaterial.getGeneratedKeys();
                        rs.next();
                        keyCache.addMaterial(namespace, materialName, subtype, rs.getShort(1));
                    }
                }
            } finally {
                keyCache.downgradeWriteLock();
            }
        }
    }
}
