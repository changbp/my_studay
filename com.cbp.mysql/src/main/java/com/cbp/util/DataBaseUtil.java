package com.cbp.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/10/8 15:30
 */
public class DataBaseUtil {
    /**
     * 关闭rs
     *
     * @param rs 关闭的对象rs
     * @Author luyan
     * @Date 2022-2-14 16:09
     */
    public static void closeResultSet(ResultSet rs) {
        if (rs == null) {
            return;
        }
        try {
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭statement
     *
     * @param statement 关闭的对象statement
     * @Author luyan
     * @Date 2022-2-14 16:09
     */
    public static void closeStatement(PreparedStatement statement) {
        if (statement == null) {
            return;
        }
        try {
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

