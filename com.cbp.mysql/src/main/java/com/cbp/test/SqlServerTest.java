package com.cbp.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/9/22 14:27
 */
public class SqlServerTest {
    public static void main(String[] args) {
        Connection conn = getConn();
        System.out.println(conn);

    }

    public static Connection getConn() {
        // 创建数据库连接 ;database=students
        String url = "jdbc:sqlserver://hadoop1:1433";
        String username = "sa";
        String password = "1234Qwer";
        Connection conn = null;
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            conn = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
}
