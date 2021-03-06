package com.amshulman.insight.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;

import com.amshulman.insight.row.BlockRowEntry;
import com.amshulman.insight.row.EntityRowEntry;
import com.amshulman.insight.row.ItemRowEntry;
import com.amshulman.insight.row.RowEntry;
import com.amshulman.insight.serialization.BlockMetadata;
import com.amshulman.insight.serialization.ItemMetadata;
import com.amshulman.insight.serialization.StorageMetadata;
import com.amshulman.insight.tbd.RowCache;
import com.amshulman.insight.types.InsightMaterial;
import com.amshulman.insight.types.MaterialCompat;
import com.amshulman.insight.util.SerializationUtil;

@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public final class DatabaseDumper implements Runnable {

    ConnectionPool cp;
    RowCache rowCache;
    ForeignKeyCache keyCache;

    @Override
    public void run() {
        if (!rowCache.isDirty()) {
            return;
        }

        Set<String> worlds = preprocessResults();
        Map<String, PreparedStatement[]> stmts = new HashMap<>(worlds.size());

        keyCache.acquireReadLock();
        try (Connection conn = cp.getConnection()) {
            conn.setAutoCommit(false);

            for (String world : worlds) {
                stmts.put(world, new PreparedStatement[] {
                    conn.prepareStatement("INSERT INTO `" + world + "_blocks`(`datetime`, `actionid`, `actorid`, `x`, `y`, `z`, `blockid`, `metadata`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"),
                    conn.prepareStatement("INSERT INTO `" + world + "_items`(`datetime`, `actionid`, `actorid`, `x`, `y`, `z`, `itemid`, `metadata`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"),
                    conn.prepareStatement("INSERT INTO `" + world + "_entities`(`datetime`, `actionid`, `actorid`, `x`, `y`, `z`, `acteeid`, `metadata`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)") }
                );
            }

            for (RowEntry row : rowCache) {
                PreparedStatement stmt;

                if (row instanceof BlockRowEntry) {
                    stmt = stmts.get(row.getWorld())[0];
                    setBasicParameters(stmt, row);

                    BlockRowEntry blockRow = (BlockRowEntry) row;
                    InsightMaterial m = MaterialCompat.getInsightMaterial(blockRow.getBlock());
                    stmt.setShort(7, keyCache.getMaterialId(m.getNamespace(), m.getName(), m.getSubtype()));

                    if (blockRow.getMetadata() == null && blockRow.getPreviousBlock() == null) {
                        stmt.setNull(8, java.sql.Types.VARBINARY);
                    } else {
                        stmt.setBytes(8, SerializationUtil.serializeMetadata(new BlockMetadata(blockRow.getMetadata(), blockRow.getPreviousBlock())));
                    }

                    stmt.addBatch();
                } else if (row instanceof ItemRowEntry) {
                    stmt = stmts.get(row.getWorld())[1];
                    setBasicParameters(stmt, row);

                    ItemRowEntry itemRow = (ItemRowEntry) row;
                    InsightMaterial m = MaterialCompat.getInsightMaterial(itemRow.getItemType(), itemRow.getDamage());
                    stmt.setShort(7, keyCache.getMaterialId(m.getNamespace(), m.getName(), (short) 0)); // damage value is stored in meta

                    StorageMetadata meta = null;
                    if (itemRow.getMetadata().serialize().size() > 1) {
                        meta = new ItemMetadata(itemRow.getMetadata(), itemRow.getQuantity(), m.getSubtype());
                    } else if (itemRow.getQuantity() > 1 || m.getSubtype() != 0) {
                        meta = new ItemMetadata(null, itemRow.getQuantity(), m.getSubtype());
                    }

                    if (meta != null) {
                        stmt.setBytes(8, SerializationUtil.serializeMetadata(meta));
                    } else {
                        stmt.setNull(8, java.sql.Types.VARBINARY);
                    }

                    stmt.addBatch();
                } else if (row instanceof EntityRowEntry) {
                    stmt = stmts.get(row.getWorld())[2];
                    setBasicParameters(stmt, row);

                    EntityRowEntry entityRow = (EntityRowEntry) row;
                    stmt.setInt(7, keyCache.getActorId(entityRow.getActee()));
                    stmt.setNull(8, java.sql.Types.VARBINARY);

                    stmt.addBatch();
                }
            }

            for(PreparedStatement[] worldStatements : stmts.values()) {
                worldStatements[0].executeBatch();
                worldStatements[1].executeBatch();
                worldStatements[2].executeBatch();
            }

            conn.setAutoCommit(true);
            rowCache.markClean();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            keyCache.releaseReadLock();
        }
    }

    private Set<String> preprocessResults() {
        Set<String> worlds = new HashSet<>(10);
        keyCache.acquireReadLock();
        try {
            // Ensure we know the row ids for all materials, actions, and actors
            // Only NPCs actors will be caught here, players are handled at login
            for (Iterator<RowEntry> iter = rowCache.iterator(); iter.hasNext();) {
                RowEntry row = iter.next();

                try {
                    if (row instanceof BlockRowEntry) {
                        InsightMaterial m = MaterialCompat.getInsightMaterial(((BlockRowEntry) row).getBlock());
                        checkMaterial(m.getNamespace(), m.getName(), m.getSubtype());
                    } else if (row instanceof ItemRowEntry) {
                        ItemRowEntry itemRow = (ItemRowEntry) row;
                        InsightMaterial m = MaterialCompat.getInsightMaterial(itemRow.getItemType(), itemRow.getDamage());
                        checkMaterial(m.getNamespace(), m.getName(), (short) 0); // damage value is stored in meta
                    } else if (row instanceof EntityRowEntry) {
                        checkActor(((EntityRowEntry) row).getActee());
                    } else {
                        continue;
                    }

                    checkActor(row.getActor());
                    checkAction(row.getAction());
                    worlds.add(row.getWorld());
                } catch (SQLException e) {
                    System.err.println(row);
                    iter.remove();
                    e.printStackTrace();
                }
            }
        } finally {
            keyCache.releaseReadLock();
        }

        return worlds;
    }

    private void setBasicParameters(PreparedStatement statement, RowEntry row) throws SQLException {
        statement.setTimestamp(1, new Timestamp(row.getDatetime()));
        statement.setByte(2, keyCache.getActionId(row.getAction()));
        statement.setInt(3, keyCache.getActorId(row.getActor()));
        statement.setInt(4, row.getX());
        statement.setShort(5, (short) Math.max(Short.MIN_VALUE, Math.min(Short.MAX_VALUE, row.getY())));
        statement.setInt(6, row.getZ());
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
