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
public class CkTest {
    public static void main(String[] args) {
        System.out.println(getConn());
    }

    public static Connection getConn() {
        // 创建数据库连接
        String url = "jdbc:clickhouse://192.168.110.101:8123";
        String username = "default";
        String password = "123456";
        Connection conn = null;
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            conn = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
}
