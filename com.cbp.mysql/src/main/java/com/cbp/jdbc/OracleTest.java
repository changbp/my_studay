package com.cbp.jdbc;

import java.sql.*;
import java.util.Arrays;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/8/14 11:05
 */
public class PostgreSqlTest {
    public static void main(String[] args) {
        Connection conn = null;
        // 加载 PostgreSQL 驱动类
        try {
            Class.forName("org.postgresql.Driver");
            // 创建数据库连接
            String url = "jdbc:postgresql://hadoop1:5432/db_2023";
            String user = "postgres";
            String password = "123456";
            conn = DriverManager.getConnection(url, user, password);
//            getVersion(conn);
//            getTotal(conn, "student");
            getColumns(conn, "student", "db_2023");

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static void getColumns(Connection conn, String tableName, String dbName) {
        ResultSet result = null;
        Statement statement = null;
        try {
            statement = conn.createStatement();
            String sql = getSql(tableName, dbName);
            System.out.println(sql);
            result = statement.executeQuery(sql);
            while (result.next()) {
                //字段注释
                String columComment = null == result.getString("detext")
                        ? "" : result.getString("detext");
                System.out.println(columComment);
                //长度限制 需要判空 默认传0
                Long length = result.getLong("length");
                if (length > 10000L) {
                    length = 0L;
                }
                System.out.println(length);
                String defaultValue = null == result.getString("defaultval")
                        ? "" : result.getString("defaultval");
                System.out.println(defaultValue);
                //默认是否为null
                Boolean isNullable ="0".equals(result.getString("is_nullable")) ? false : true;
                System.out.println(isNullable);
                //是否为主键
                String ispk = result.getString("ispk");
                System.out.println("---------ispk-------" + ispk);
                Boolean isPk = "0".equals(result.getString("ispk")) ? false : true;
                System.out.println(isPk);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (result != null) {
                    result.close();
                }
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 拼接sql(编码规则，一个方法不允许超过80行)
     */
    public static String getSql(String tableName, String dbName) {
        String sql = "SELECT "
                + " ordinal_position AS Colorder, "
                + " COLUMN_NAME AS ColumnName, "
                + " data_type AS TypeName, "
                + " COALESCE ( character_maximum_length, numeric_precision,- 1 ) AS LENGTH, "
                + " numeric_scale AS SCALE, "
                + "CASE is_nullable  WHEN 'NO' THEN  0 ELSE 1   END AS is_nullable, "
                + " column_default AS DefaultVal, "
                + "CASE  WHEN POSITION ( 'nextval' IN column_default ) > 0 THEN   1 ELSE 0  END AS IsIdentity, "
                + "CASE  WHEN b.pk_name IS NULL THEN  0 ELSE 1  END AS IsPK, "
                + " C.DeText  "
                + "FROM "
                + " information_schema.COLUMNS LEFT JOIN ( "
                + " SELECT "
                + "  pg_attr.attname AS colname, "
                + "  pg_constraint.conname AS pk_name  "
                + " FROM "
                + "  pg_constraint "
                + "  INNER JOIN pg_class ON pg_constraint.conrelid = pg_class.oid "
                + "  INNER JOIN pg_attribute pg_attr ON pg_attr.attrelid = pg_class.oid  "
                + "  AND pg_attr.attnum = pg_constraint.conkey [ 1 ] "
                + "  INNER JOIN pg_type ON pg_type.oid = pg_attr.atttypid  "
                + " WHERE "
                + "  pg_class.relname = '" + tableName + "' "
                + "  AND pg_constraint.contype = 'p'  "
                + " ) b ON b.colname = information_schema.COLUMNS.COLUMN_NAME LEFT JOIN ( "
                + " SELECT "
                + "  attname, "
                + "  description AS DeText  "
                + " FROM "
                + "  pg_class "
                + "  LEFT JOIN pg_attribute pg_attr ON pg_attr.attrelid = pg_class.oid "
                + "  LEFT JOIN pg_description pg_desc ON pg_desc.objoid = pg_attr.attrelid  "
                + "  AND pg_desc.objsubid = pg_attr.attnum  "
                + " WHERE "
                + "  pg_attr.attnum > 0  "
                + "  AND pg_attr.attrelid = pg_class.oid  "
                + "  AND pg_class.relname = '"
                + tableName
                + "' "
                + " ) C ON C.attname = information_schema.COLUMNS.COLUMN_NAME  "
                + "WHERE "
                + " table_catalog = '"
                + dbName
                + "' "
                + " AND table_schema = 'public'  "
                + " AND TABLE_NAME = '"
                + tableName
                + "' "
                + "ORDER BY "
                + " ordinal_position ASC";
        return sql;
    }

    public static void getTotal(Connection conn, String tableName) {
        Statement statement = null;
        ResultSet result = null;
        try {
            statement = conn.createStatement();
            String sql = "select count(1) as total from " + tableName;
            result = statement.executeQuery(sql);
            while (result.next()) {
                String total = String.valueOf(result.getLong("total"));
                System.out.println(total);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (result != null) {
                    result.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void getVersion(Connection conn) {
        Statement statement = null;
        ResultSet result = null;
        try {
            statement = conn.createStatement();
            result = statement.executeQuery("SELECT split_part(version(), ',', 1) as version");
            while (result.next()) {
                String[] versions = result.getString("version").split("-");
                boolean flag = Arrays.toString(versions).contains("linux");
                if (flag) {
                    System.out.println("Linux");
                } else {
                    System.out.println("window");
                }

            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (result != null) {
                    result.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


}
