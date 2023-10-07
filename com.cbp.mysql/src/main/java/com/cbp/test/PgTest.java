package com.cbp.test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/9/22 14:26
 */
public class PgTest {
    public static void main(String[] args) {
        List<String> dataBaseNameList = getDataBaseNameList(getConn());
        System.out.println(dataBaseNameList);
        System.out.println("=========================databases==========================");
        if (dataBaseNameList != null) {
            dataBaseNameList.forEach(dataBase -> {
                List<String> tableNameList = getTableNameList(getConn(), dataBase);
                System.out.println(tableNameList);
                System.out.println("=========================tableNames==========================" + dataBase);
            });
        }

    }

    public static List<String> getTableNameList(Connection conn, String dbName) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<String> tableNameList = new ArrayList<>();
        String dbQuery = "SELECT " +
                " table_name " +
                "FROM " +
                " information_schema.tables  " +
                "WHERE " +
                " table_catalog = '" + dbName + "' and table_schema = 'public'";
        try {
            statement = conn.prepareStatement(dbQuery);
            rs = statement.executeQuery();
            while (rs.next()) {
                tableNameList.add(rs.getString(1));
            }
            return tableNameList;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (statement != null) {
                    statement.close();
                }
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static List<String> getDataBaseNameList(Connection conn) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<String> dbNameList = new ArrayList<>();
        String dbQuery =
                "SELECT datname FROM pg_database WHERE datistemplate = 'f';";
        try {
            statement = conn.prepareStatement(dbQuery);
            rs = statement.executeQuery();
            while (rs.next()) {
                dbNameList.add(rs.getString(1));
            }
            return dbNameList;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (rs != null) {
                    rs.close();
                }
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static Connection getConn() {
        // 创建数据库连接
        String url = "jdbc:postgresql://hadoop1:5432/db_2023";
        String username = "postgres";
        String password = "123456";
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
}
