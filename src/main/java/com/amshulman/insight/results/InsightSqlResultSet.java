package com.amshulman.insight.results;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.amshulman.insight.query.QueryParameters;
import com.amshulman.insight.types.EventCompat;
import com.amshulman.insight.types.InsightMaterial;
import com.amshulman.insight.util.SerializationUtil;

public class InsightSqlResultSet extends InsightResultSet {

    public InsightSqlResultSet(ResultSet rs, QueryParameters params) throws SQLException {
        super(params);
        while (rs.next()) {
            add(new InsightRecord(
                  rs.getTimestamp("datetime"),
                  rs.getString("actor"),
                  EventCompat.getActionByName(rs.getString("action")),
                  rs.getInt("x"),
                  rs.getInt("y"),
                  rs.getInt("z"),
                  rs.getString("world"),
                  new InsightMaterial(rs.getString("material_namespace"), rs.getString("material_name"), rs.getShort("material_subtype")),
                  rs.getString("actee"),
                  SerializationUtil.deserializeMetadata(rs.getBytes("metadata"))));
        }

        doneAdding();
    }

    public InsightSqlResultSet(List<InsightRecord> subList, QueryParameters params) {
        super(subList, params);
    }

    @Override
    public InsightResultSet getResultSubset(int fromIndex, int toIndex) {
        return new InsightSqlResultSet(getSubList(fromIndex, toIndex), this.getQueryParameters());
    }
}
