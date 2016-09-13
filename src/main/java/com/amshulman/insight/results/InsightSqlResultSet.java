package com.amshulman.insight.results;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.amshulman.insight.action.BlockAction;
import com.amshulman.insight.action.InsightAction;
import com.amshulman.insight.action.ItemAction;
import com.amshulman.insight.query.QueryParameters;
import com.amshulman.insight.serialization.ItemMetadata;
import com.amshulman.insight.serialization.StorageMetadata;
import com.amshulman.insight.types.EventRegistry;
import com.amshulman.insight.types.InsightLocation;
import com.amshulman.insight.types.InsightMaterial;
import com.amshulman.insight.util.SerializationUtil;

public class InsightSqlResultSet extends InsightResultSet {

    public InsightSqlResultSet(ResultSet rs, QueryParameters params) throws SQLException {
        super(params);
        while (rs.next()) {
            InsightAction action = EventRegistry.getActionByName(rs.getString("action"));
            StorageMetadata meta = null;
            try {
                meta = SerializationUtil.deserializeMetadata(rs.getBytes("metadata"));
            } catch (IllegalArgumentException e) {
                // Nothing useful to do here
            }

            InsightMaterial material;
            if (action instanceof BlockAction) {
                material = new InsightMaterial(rs.getString("material_namespace"),
                                               rs.getString("material_name"),
                                               rs.getShort("material_subtype"));
            } else if (action instanceof ItemAction) {
                material = new InsightMaterial(rs.getString("material_namespace"),
                                               rs.getString("material_name"),
                                               meta == null ? 0 : ((ItemMetadata) meta).getDamage());
            } else {
                material = null;
            }

            add(new InsightRecord<InsightAction>(
                  rs.getTimestamp("datetime"),
                  rs.getString("actor"),
                  action,
                  new InsightLocation(rs.getInt("x"), rs.getInt("y"), rs.getInt("z"), rs.getString("world")),
                  material,
                  rs.getString("actee"),
                  meta));
        }

        doneAdding();
    }

    private InsightSqlResultSet(List<InsightRecord<?>> subList, QueryParameters params) {
        super(subList, params);
    }

    @Override
    public InsightResultSet getResultSubset(int fromIndex, int toIndex) {
        return new InsightSqlResultSet(getSubList(fromIndex, toIndex), this.getQueryParameters());
    }
}
