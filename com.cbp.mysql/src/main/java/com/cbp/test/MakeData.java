package com.cbp.test;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * @Author changbp
 * @Date 2021-04-14 16:42
 * @Return
 * @Version 1.0
 */
public class MakeData {
    public static void main(String[] args) {
        String date = "2023-08-07 14:13:55.227";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        try {
            System.out.println(simpleDateFormat.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
//        try {
            //数据库连接池使用
//            HikariConfig config = new HikariConfig();
//            config.setJdbcUrl("jdbc:mysql://localhost:3306/test");
//            config.setUsername("root");
//            config.setPassword("password");
//            config.addDataSourceProperty("connectionTimeout", "1000"); // 连接超时：1秒
//            config.addDataSourceProperty("idleTimeout", "60000"); // 空闲超时：60秒
//            config.addDataSourceProperty("maximumPoolSize", "10"); // 最大连接数：10
//            DataSource ds = new HikariDataSource(config);
//            Connection connection1 = ds.getConnection();
//            String url = "jdbc:mysql://192.168.1.215:3306/test";
//            String username = "root";
//            String password = "123456";
//            Class.forName("com.mysql.cj.jdbc.Driver");
//            Connection connection = DriverManager.getConnection(url, username, password);
//
//            String sql = "INSERT INTO userinfo (" +
//                    "id,zjhm,name,age,sex,nation,csdm,birthday,workingtime,politicsstatus,hkszdm,email,phone,urgencypeple,postcode,address,ismarried,workingyears,hobby,corporation,beizhu " +
//                    ")" +
//                    "value(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? )";
//
//            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//            connection.setAutoCommit(false);
//            for (int j = 1; j <= 26; j++) {
//                PreparedStatement pst = connection.prepareStatement(sql);
//                for (int i = 0; i < 2000000; i++) {
//                    pst.setString(1, UUID.randomUUID().toString());
//                    pst.setString(2, UUID.randomUUID().toString());
//                    pst.setString(3, "刘亦菲" + i);
//                    pst.setInt(4, 18);
//                    pst.setString(5, "1");
//                    pst.setString(6, "汉族");
//                    pst.setString(7, UUID.randomUUID().toString());
//                    pst.setString(8, simpleDateFormat.format(new Date()));
//                    pst.setString(9, simpleDateFormat.format(new Date()));
//                    pst.setString(10, "团员");
//                    pst.setString(11, "北京市昌平区东小口西城家园");
//                    pst.setString(12, "lyf@163.com");
//                    pst.setString(13, "15201234567");
//                    pst.setString(14, "15202345678");
//                    pst.setString(15, "000000");
//                    pst.setString(16, "北京市昌平区东小口西城家园");
//                    pst.setString(17, "0");
//                    pst.setInt(18, 4);
//                    pst.setString(19, "唱歌");
//                    pst.setString(20, "北京北大软件科技有限公司");
//                    pst.setString(21, "美女一枚");
//                    pst.addBatch();
//                }
//                pst.executeBatch();
//                connection.commit();
//                System.out.println(j + "次");
//                Thread.sleep(10000);
//                pst.close();
//            }
//            connection.close();
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//        }
    }
}
