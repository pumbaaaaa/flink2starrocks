package com.pumbaa.starrocks;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class JdbcUtil {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static List<JSONObject> executeQuery(String hostUrl, int port, String user, String password, String sql) {
        List<JSONObject> beJson = new ArrayList<>();
        String connectionUrl = String.format("jdbc:mysql://%s:%s/", hostUrl, port);
        Connection con = null;
        try {
            con = DriverManager.getConnection(connectionUrl, user, password);
            PreparedStatement ps = con.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            beJson = resultSetToJson(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                con.close();
            } catch (Exception e) {
            }
        }
        return beJson;
    }

    private static List<JSONObject> resultSetToJson(ResultSet rs) throws SQLException {
        List<JSONObject> list = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                String value = rs.getString(columnName);
                jsonObj.put(columnName, value);
            }
            list.add(jsonObj);
        }
        return list;
    }
}
