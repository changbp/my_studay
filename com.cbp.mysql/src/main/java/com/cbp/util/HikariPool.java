package com.cbp.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ResourceBundle;

/**
 * @Author changbp
 * @Date 2021-06-04 11:11
 * @implNote HikariDataSource 数据库连接池使用
 * @Version 1.0
 */
public class HikariPool {


    public static void main(String[] args) {
        System.out.println(getShowData("config"));
    }


    /*
     * 获取数据
     * */
    public static String getShowData(String propertiesName) {
        String resultData = "";
        try (Connection connection = getConnection(propertiesName)) {
            String sql = "select *  from dc_metadatatable limit 2 ";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                try (ResultSet resultSet = ps.executeQuery();) {
                    resultData = getRsultData(resultSet);
                }
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return resultData;
    }

    public static final String URL = "url";
    public static final String USER = "user";
    public static final String PASSWD = "passwd";
    public static final String CONNECTIONTIMEOUT = "connectionTimeout";
    public static final String IDLETIMEOUT = "idleTimeout";
    public static final String MAXIMUMPOOLSIZE = "maximumPoolSize";
    /*
     * 获取数据库连接
     * */
    public static Connection getConnection(String propertiesName) throws SQLException {
        final ResourceBundle config = ReadProperties.getConfig(propertiesName);
        //数据库连接池使用
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getString(URL));
        hikariConfig.setUsername(USER);
        hikariConfig.setPassword(PASSWD);
        hikariConfig.addDataSourceProperty("connectionTimeout", CONNECTIONTIMEOUT); // 连接超时：1秒
        hikariConfig.addDataSourceProperty("idleTimeout", IDLETIMEOUT); // 空闲超时：60秒
        hikariConfig.addDataSourceProperty("maximumPoolSize", MAXIMUMPOOLSIZE); // 最大连接数：10
        DataSource ds = new HikariDataSource(hikariConfig);
        return ds.getConnection();
    }

    /*
     * 遍历结果集 resultSet
     * */
    public static String getRsultData(ResultSet resultSet) throws SQLException {
        JSONArray resArray = new JSONArray();
        while (resultSet.next()) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            JSONObject showObj = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                Object value = resultSet.getObject(i);
                showObj.put(columnName, value);
            }
            resArray.add(showObj);
        }
        return resArray.toString();
    }

}
