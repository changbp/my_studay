package com.cbp.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/10/11 14:09
 */
public class OracleTest {
    public static void main(String[] args) {
        System.out.println(getConn());
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
}
