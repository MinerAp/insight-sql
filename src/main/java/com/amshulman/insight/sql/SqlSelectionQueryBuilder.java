package com.amshulman.insight.sql;

import gnu.trove.map.TIntByteMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TIntShortMap;
import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TIntShortHashMap;
import gnu.trove.procedure.TIntByteProcedure;
import gnu.trove.procedure.TIntIntProcedure;
import gnu.trove.procedure.TIntObjectProcedure;
import gnu.trove.procedure.TIntShortProcedure;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;

import lombok.AllArgsConstructor;

import com.amshulman.insight.action.BlockAction;
import com.amshulman.insight.action.EntityAction;
import com.amshulman.insight.action.InsightAction;
import com.amshulman.insight.action.ItemAction;
import com.amshulman.insight.query.QueryParameters;
import com.amshulman.insight.sql.TroveLoops.AddWhereClauseByteParameters;
import com.amshulman.insight.sql.TroveLoops.AddWhereClauseIntParameters;
import com.amshulman.insight.sql.TroveLoops.AddWhereClauseObjectParameters;
import com.amshulman.insight.sql.TroveLoops.AddWhereClauseShortParameters;
import com.amshulman.insight.types.InsightMaterial;

public class SqlSelectionQueryBuilder {

    private static final String EMPTY_STRING = "";
    private static final String AND = " AND ";
    private static final String OR = " OR ";
    private static final String NOT = " NOT ";
    private static final String EQUALS = " = ";
    private static final String NOT_EQUALS = " != ";
    private static final String BETWEEN = " BETWEEN ";
    private static final char LEFT_PAREN = '(';
    private static final char RIGHT_PAREN = ')';
    private static final char PARAM = '?';

    private static final TIntObjectMap<String> whereClauseStringParams = new TIntObjectHashMap<String>();
    private static final TIntObjectMap<Date> whereClauseTimeParams = new TIntObjectHashMap<Date>();
    private static final TIntIntMap whereClauseIntParams = new TIntIntHashMap();
    private static final TIntShortMap whereClauseShortParams = new TIntShortHashMap();
    private static final TIntByteMap whereClauseByteParams = new TIntByteHashMap();

    private static final TIntObjectMap<String> queryStringParams = new TIntObjectHashMap<String>();
    private static final TIntObjectMap<Date> queryTimeParams = new TIntObjectHashMap<Date>();
    private static final TIntIntMap queryIntParams = new TIntIntHashMap();
    private static final TIntShortMap queryShortParams = new TIntShortHashMap();
    private static final TIntByteMap queryByteParams = new TIntByteHashMap();

    public static SqlSelectionQuery build(QueryParameters params) {
        if (params.getWorlds().isEmpty()) {
            throw new IllegalStateException("You must query at least one world");
        }

        whereClauseStringParams.clear();
        whereClauseTimeParams.clear();
        whereClauseIntParams.clear();
        whereClauseShortParams.clear();
        whereClauseByteParams.clear();

        queryStringParams.clear();
        queryTimeParams.clear();
        queryIntParams.clear();
        queryShortParams.clear();
        queryByteParams.clear();

        String whereClause = buildWhereClause(params);
        String query = buildSelectClauses(params, whereClause);
        query += " ORDER BY `datetime`";

        if (params.isReverseOrder()) {
            query += " ASC";
        } else {
            query += " DESC";
        }

        return new SqlSelectionQuery(query, queryStringParams, queryTimeParams, queryIntParams, queryShortParams, queryByteParams);
    }

    private static String buildWhereClause(QueryParameters params) {
        StringBuilder sb = new StringBuilder();
        int paramIndex = 1;

        paramIndex = appendActors(sb, params.getActors(), params.isInvertActors(), "`actors`.`name`", paramIndex);

        paramIndex = appendActions(sb, params, paramIndex);

        paramIndex = appendActors(sb, params.getActees(), params.isInvertActees(), "`actees`.`name`", paramIndex);

        paramIndex = appendMaterials(sb, params, paramIndex);

        paramIndex = appendLocation(sb, params, paramIndex);

        paramIndex = appendTime(sb, params, paramIndex);

        if (sb.length() != 0) {
            return sb.insert(0, " WHERE ").toString();
        } else {
            return "";
        }
    }

    private static int appendActors(StringBuilder sb, Set<String> set, boolean invert, String field, int initialParamIndex) {
        int paramIndex = initialParamIndex;

        if (!set.isEmpty()) {
            Iterator<String> iter = set.iterator();
            String equality, conjunction;

            if (invert) {
                equality = NOT_EQUALS;
                conjunction = AND;
            } else {
                equality = EQUALS;
                conjunction = OR;
            }

            if (sb.length() != 0) {
                sb.append(AND);
            }

            sb.append(LEFT_PAREN + field + equality + PARAM);
            whereClauseStringParams.put(paramIndex++, iter.next());
            while (iter.hasNext()) {
                sb.append(conjunction + field + equality + PARAM);
                whereClauseStringParams.put(paramIndex++, iter.next());
            }
            sb.append(RIGHT_PAREN);
        }

        return paramIndex;
    }

    private static int appendActions(StringBuilder sb, QueryParameters params, int initialParamIndex) {
        int paramIndex = initialParamIndex;

        if (!params.getActions().isEmpty()) {
            Iterator<InsightAction> iter = params.getActions().iterator();
            String equality, conjunction;
            String field = "`actions`.`name`";

            if (params.isInvertActions()) {
                equality = NOT_EQUALS;
                conjunction = AND;
            } else {
                equality = EQUALS;
                conjunction = OR;
            }

            if (sb.length() != 0) {
                sb.append(AND);
            }

            sb.append(LEFT_PAREN + field + equality + PARAM);
            whereClauseStringParams.put(paramIndex++, iter.next().getName());
            while (iter.hasNext()) {
                sb.append(conjunction + field + equality + PARAM);
                whereClauseStringParams.put(paramIndex++, iter.next().getName());
            }
            sb.append(RIGHT_PAREN);
        }

        return paramIndex;
    }

    private static int appendMaterials(StringBuilder sb, QueryParameters params, int initialParamIndex) {
        int paramIndex = initialParamIndex;

        if (!params.getMaterials().isEmpty()) {
            Iterator<InsightMaterial> iter = params.getMaterials().iterator();
            String equality, conjunction;
            String namespaceField = "`materials`.`namespace`",
                   nameField = "`materials`.`name`",
                   subtypeField = "`materials`.`subtype`";

            if (params.isInvertMaterials()) {
                equality = NOT;
                conjunction = AND;
            } else {
                equality = EMPTY_STRING;
                conjunction = OR;
            }

            if (sb.length() != 0) {
                sb.append(AND);
            }

            InsightMaterial mat = iter.next();
            sb.append(LEFT_PAREN);

            whereClauseStringParams.put(paramIndex++, mat.getNamespace());
            whereClauseStringParams.put(paramIndex++, mat.getName());

            sb.append(equality + LEFT_PAREN +
                      namespaceField + EQUALS + PARAM + AND +
                      nameField + EQUALS + PARAM);

            if (mat.getSubtype() != InsightMaterial.UNSPECIFIED_SUBTYPE) {
                whereClauseShortParams.put(paramIndex++, mat.getSubtype());
                sb.append(AND + subtypeField + EQUALS + PARAM);
            }
            sb.append(RIGHT_PAREN);

            while (iter.hasNext()) {
                mat = iter.next();
                whereClauseStringParams.put(paramIndex++, mat.getNamespace());
                whereClauseStringParams.put(paramIndex++, mat.getName());

                sb.append(conjunction + equality + LEFT_PAREN +
                          namespaceField + EQUALS + PARAM + AND +
                          nameField + EQUALS + PARAM);
                if (mat.getSubtype() != InsightMaterial.UNSPECIFIED_SUBTYPE) {
                    whereClauseShortParams.put(paramIndex++, mat.getSubtype());
                    sb.append(AND + subtypeField + EQUALS + PARAM);
                }
                sb.append(RIGHT_PAREN);
            }
            sb.append(RIGHT_PAREN);
        }

        return paramIndex;
    }

    private static int appendLocation(StringBuilder sb, QueryParameters params, int initialParamIndex) {
        int paramIndex = initialParamIndex;

        if (params.isLocationSet()) {
            if (sb.length() != 0) {
                sb.append(AND);
            }

            if (params.getPoint() == null || params.getRadius() != 0) {
                sb.append(LEFT_PAREN +
                          "`x`" + BETWEEN + PARAM + AND + PARAM + AND +
                          "`y`" + BETWEEN + PARAM + AND + PARAM + AND +
                          "`z`" + BETWEEN + PARAM + AND + PARAM);
                whereClauseIntParams.put(paramIndex++, params.getMinX());
                whereClauseIntParams.put(paramIndex++, params.getMaxX());
                whereClauseIntParams.put(paramIndex++, params.getMinY());
                whereClauseIntParams.put(paramIndex++, params.getMaxY());
                whereClauseIntParams.put(paramIndex++, params.getMinZ());
                whereClauseIntParams.put(paramIndex++, params.getMaxZ());

                if (params.getPoint() != null) {
                    sb.append(AND + "NEAR" + LEFT_PAREN +
                              "`x`, " + PARAM + ", " +
                              "`y`, " + PARAM + ", " +
                              "`z`, " + PARAM + ", " +
                              PARAM + RIGHT_PAREN);

                    whereClauseIntParams.put(paramIndex++, params.getPoint().getBlockX());
                    whereClauseIntParams.put(paramIndex++, params.getPoint().getBlockY());
                    whereClauseIntParams.put(paramIndex++, params.getPoint().getBlockZ());
                    whereClauseIntParams.put(paramIndex++, params.getRadius() * params.getRadius());
                }

                sb.append(RIGHT_PAREN);
            } else {
                sb.append(LEFT_PAREN +
                          "`x`" + EQUALS + PARAM + AND +
                          "`y`" + EQUALS + PARAM + AND +
                          "`z`" + EQUALS + PARAM + RIGHT_PAREN);

                whereClauseIntParams.put(paramIndex++, params.getPoint().getBlockX());
                whereClauseIntParams.put(paramIndex++, params.getPoint().getBlockY());
                whereClauseIntParams.put(paramIndex++, params.getPoint().getBlockZ());
            }
        }

        return paramIndex;
    }

    private static int appendTime(StringBuilder sb, QueryParameters params, int initalParamIndex) {
        int paramIndex = initalParamIndex;

        if (params.getAfter() != null) {
            if (sb.length() != 0) {
                sb.append(AND);
            }

            sb.append("`datetime` > " + PARAM);
            whereClauseTimeParams.put(paramIndex++, params.getAfter());
        }

        if (params.getBefore() != null) {
            if (sb.length() != 0) {
                sb.append(AND);
            }

            sb.append("`datetime` < " + PARAM);
            whereClauseTimeParams.put(paramIndex++, params.getBefore());
        }

        return paramIndex;
    }

    private static String buildSelectClauses(QueryParameters params, String whereClause) {
        int paramIndex = 1;
        String[] perWorldQueries = new String[params.getWorlds().size()];

        boolean blockActions = false;
        boolean entityActions = false;
        boolean itemActions = false;

        if (!params.getMaterials().isEmpty() && !params.getActees().isEmpty()) {
            throw new IllegalArgumentException("You cannot specify both materials and actees");
        }

        if (params.getActions().isEmpty()) {
            if (!params.getMaterials().isEmpty()) {
                blockActions = true;
                entityActions = false;
                itemActions = true;
            } else if (!params.getActees().isEmpty()) {
                blockActions = false;
                entityActions = true;
                itemActions = false;
            } else {
                blockActions = true;
                entityActions = true;
                itemActions = true;
            }
        } else {
            for (InsightAction a : params.getActions()) {
                if (a instanceof BlockAction) {
                    blockActions = true;
                } else if (a instanceof EntityAction) {
                    entityActions = true;
                } else if (a instanceof ItemAction) {
                    itemActions = true;
                }
            }

            if (entityActions && !params.getMaterials().isEmpty()) {
                throw new IllegalArgumentException("You cannot specify both materials and entity actions");
            }

            if ((blockActions || itemActions) && !params.getActees().isEmpty()) {
                throw new IllegalArgumentException("You cannot specify both actees and item or block actions");
            }
        }

        int i = 0;
        for (String world : params.getWorlds()) {
            String query = "";

            if (blockActions) {
                String subquery =
                        "SELECT `datetime`, `actors`.`name` AS `actor`, `actions`.`name` AS `action`, `x`, `y`, `z`, '" + world + "' as `world`, " +
                                "`materials`.`namespace` AS `material_namespace`, `materials`.`name` AS `material_name`, `materials`.`subtype` AS `material_subtype`, " +
                                "NULL AS `actee`, `metadata` " +
                                "FROM `" + world + "_blocks` " +
                                "INNER JOIN `actors` ON `" + world + "_blocks`.`actorid` = `actors`.`id` " +
                                "INNER JOIN `actions` ON `" + world + "_blocks`.`actionid` = `actions`.`id` " +
                                "INNER JOIN `materials` ON `" + world + "_blocks`.`blockid` = `materials`.`id`";

                query = subquery + whereClause;
                whereClauseStringParams.forEachEntry(AddWhereClauseObjectParameters.with(queryStringParams, paramIndex));
                whereClauseTimeParams.forEachEntry(AddWhereClauseObjectParameters.with(queryTimeParams, paramIndex));
                whereClauseIntParams.forEachEntry(AddWhereClauseIntParameters.with(queryIntParams, paramIndex));
                whereClauseShortParams.forEachEntry(AddWhereClauseShortParameters.with(queryShortParams, paramIndex));
                whereClauseByteParams.forEachEntry(AddWhereClauseByteParameters.with(queryByteParams, paramIndex));
                paramIndex += whereClauseStringParams.size() + whereClauseTimeParams.size() + whereClauseIntParams.size() + whereClauseShortParams.size() + whereClauseByteParams.size();
            }

            if (entityActions) {
                String subquery =
                        "SELECT `datetime`, `actors`.`name` AS `actor`, `actions`.`name` AS `action`, `x`, `y`, `z`, '" + world + "' as `world`, " +
                                "NULL AS `material_namespace`, NULL AS `material_name`, NULL AS `material_subtype`, " +
                                "`actees`.`name` AS `actee`, `metadata` " +
                                "FROM `" + world + "_entities` " +
                                "INNER JOIN `actors` ON `" + world + "_entities`.`actorid` = `actors`.`id` " +
                                "INNER JOIN `actions` ON `" + world + "_entities`.`actionid` = `actions`.`id` " +
                                "INNER JOIN `actors` AS `actees` ON `" + world + "_entities`.`acteeid` = `actees`.`id`";

                if (!query.isEmpty()) {
                    query += " UNION ALL " + subquery + whereClause;
                } else {
                    query = subquery + whereClause;
                }
                whereClauseStringParams.forEachEntry(AddWhereClauseObjectParameters.with(queryStringParams, paramIndex));
                whereClauseTimeParams.forEachEntry(AddWhereClauseObjectParameters.with(queryTimeParams, paramIndex));
                whereClauseIntParams.forEachEntry(AddWhereClauseIntParameters.with(queryIntParams, paramIndex));
                whereClauseShortParams.forEachEntry(AddWhereClauseShortParameters.with(queryShortParams, paramIndex));
                whereClauseByteParams.forEachEntry(AddWhereClauseByteParameters.with(queryByteParams, paramIndex));
                paramIndex += whereClauseStringParams.size() + whereClauseTimeParams.size() + whereClauseIntParams.size() + whereClauseShortParams.size() + whereClauseByteParams.size();
            }

            if (itemActions) {
                String subquery =
                        "SELECT `datetime`, `actors`.`name` AS `actor`, `actions`.`name` AS `action`, `x`, `y`, `z`, '" + world + "' as `world`, " +
                                "`materials`.`namespace` AS `material_namespace`, `materials`.`name` AS `material_name`, `materials`.`subtype` AS `material_subtype`, " +
                                "NULL AS `actee`, `metadata` " +
                                "FROM `" + world + "_items` " +
                                "INNER JOIN `actors` ON `" + world + "_items`.`actorid` = `actors`.`id` " +
                                "INNER JOIN `actions` ON `" + world + "_items`.`actionid` = `actions`.`id` " +
                                "INNER JOIN `materials` ON `" + world + "_items`.`itemid` = `materials`.`id`";

                if (!query.isEmpty()) {
                    query += " UNION ALL " + subquery + whereClause;
                } else {
                    query = subquery + whereClause;
                }
                whereClauseStringParams.forEachEntry(AddWhereClauseObjectParameters.with(queryStringParams, paramIndex));
                whereClauseTimeParams.forEachEntry(AddWhereClauseObjectParameters.with(queryTimeParams, paramIndex));
                whereClauseIntParams.forEachEntry(AddWhereClauseIntParameters.with(queryIntParams, paramIndex));
                whereClauseShortParams.forEachEntry(AddWhereClauseShortParameters.with(queryShortParams, paramIndex));
                whereClauseByteParams.forEachEntry(AddWhereClauseByteParameters.with(queryByteParams, paramIndex));
                paramIndex += whereClauseStringParams.size() + whereClauseTimeParams.size() + whereClauseIntParams.size() + whereClauseShortParams.size() + whereClauseByteParams.size();
            }

            perWorldQueries[i] = query;
            ++i;
        }

        String query = perWorldQueries[0];
        for (i = 1; i < perWorldQueries.length; ++i) {
            query += " UNION ALL " + perWorldQueries[1];
        }

        return query;
    }

    @AllArgsConstructor
    public static final class SqlSelectionQuery {

        private final String sql;
        private final TIntObjectMap<String> stringParams;
        private final TIntObjectMap<Date> timeParams;
        private final TIntIntMap intParams;
        private final TIntShortMap shortParams;
        private final TIntByteMap byteParams;

        public PreparedStatement getPreparedStatement(Connection c) throws SQLException {
            final PreparedStatement stmt = c.prepareStatement(sql);
            boolean status = true;

            status &= stringParams.forEachEntry(new TIntObjectProcedure<String>() {

                public boolean execute(int a, String b) {
                    try {
                        stmt.setString(a, b);
                    } catch (SQLException e) {
                        return false;
                    }
                    return true;
                }
            });

            status &= timeParams.forEachEntry(new TIntObjectProcedure<Date>() {

                public boolean execute(int a, Date b) {
                    try {
                        stmt.setTimestamp(a, new Timestamp(b.getTime()));
                    } catch (SQLException e) {
                        return false;
                    }
                    return true;
                }
            });

            status &= intParams.forEachEntry(new TIntIntProcedure() {

                public boolean execute(int a, int b) {
                    try {
                        stmt.setInt(a, b);
                    } catch (SQLException e) {
                        return false;
                    }
                    return true;
                }
            });

            status &= shortParams.forEachEntry(new TIntShortProcedure() {

                public boolean execute(int a, short b) {
                    try {
                        stmt.setShort(a, b);
                    } catch (SQLException e) {
                        return false;
                    }
                    return true;
                }
            });

            status &= byteParams.forEachEntry(new TIntByteProcedure() {

                public boolean execute(int a, byte b) {
                    try {
                        stmt.setByte(a, b);
                    } catch (SQLException e) {
                        return false;
                    }
                    return true;
                }
            });

            if (!status) {
                throw new SQLException("Error parameterizing statement:\n"
                        + sql
                        + "\nstringParams: " + stringParams.toString()
                        + "\ntimeParams: " + timeParams.toString()
                        + "\nintParams: " + intParams.toString()
                        + "\nbyteParams: " + byteParams.toString());
            }

            return stmt;
        }
    }
}
