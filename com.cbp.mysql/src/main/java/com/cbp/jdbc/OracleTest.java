package com.cbp.jdbc;

import com.cbp.util.DataBaseUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/8/14 11:05
 */
public class OracleTest {
    public static void main(String[] args) {
        Connection conn = getConn();
        List<String> dataBaseNameList = getDataBaseNameList(conn);
        if (dataBaseNameList != null) {
            dataBaseNameList.forEach(System.out::println);
        }
    }

    public static Connection getConn() {
        // 创建数据库连接
        String url = "jdbc:oracle:thin:@192.168.110.101:1521:helowin";
        String username = "system";
        String password = "oracle";
        Connection conn = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static List<String> getDataBaseNameList(Connection conn) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<String> dbNameList = new ArrayList<>();
        //String dbQuery = "select username from sys.dba_users";
        String dbQuery = "select distinct owner from all_objects";
        try {
            statement = conn.prepareStatement(dbQuery);
            rs = statement.executeQuery();
            while (rs.next()) {
                dbNameList.add(rs.getString(1));
            }
            return dbNameList;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            DataBaseUtil.closeResultSet(rs);
            DataBaseUtil.closeStatement(statement);
        }
        return null;
    }

    public List<String> getTableNameList(Connection conn, String dbName) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<String> tableNameList = new ArrayList<>();
        String dbQuery = "select table_name from dba_tables where owner = '" + dbName + "'";
        try {
            statement = conn.prepareStatement(dbQuery);
            rs = statement.executeQuery();
            while (rs.next()) {
                tableNameList.add(rs.getString(1));
            }
        } catch (SQLException throwables) {
            dbQuery = "select table_name from user_tables";
            try {
                statement = conn.prepareStatement(dbQuery);
                rs = statement.executeQuery();
                tableNameList = new ArrayList<>();
                while (rs.next()) {
                    tableNameList.add(rs.getString(1));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } finally {
            DataBaseUtil.closeResultSet(rs);
            DataBaseUtil.closeStatement(statement);
        }
        return tableNameList;
    }

    public List<String> getColumnNameList(Connection conn, String dbName, String tableName) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<String> columnList = new ArrayList<>();
        String dbQuery = "select * from \"" + dbName + "\".\"" + tableName + "\" where 1=0";
        try {
            statement = conn.prepareStatement(dbQuery);
            rs = statement.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            //查询字段数
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                if (columnName.contains(".")) {
                    columnList.add(columnName.substring(columnName.indexOf(".") + 1)
                            + ":" + metaData.getColumnTypeName(i));
                } else {
                    columnList.add(columnName + ":" + metaData.getColumnTypeName(i));
                }
            }
            Collections.sort(columnList);
            return columnList;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            DataBaseUtil.closeResultSet(rs);
            DataBaseUtil.closeStatement(statement);
        }
        return null;
    }


}
