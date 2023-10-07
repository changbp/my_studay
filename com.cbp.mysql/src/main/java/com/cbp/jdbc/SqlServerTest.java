package com.cbp.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/8/14 10:14
 */
public class SqlServerTest {
    public static void main(String[] args) {
        Connection con = null;
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerXADataSource");
            con = DriverManager.getConnection("jdbc:sqlserver://hadoop1:1433;DatabaseName=students", "sa", "1234Qwer");
//            List<String> tableNameList = getTableNameList(con, "students", "");
//            System.out.println(tableNameList.toString());
//
//            List<String> student = getColumnNameList(con, "", "", "student");
//            System.out.println(student.toString());
//
//            getVersion(con);
//            getCountNum(con, "student");
            getColumns(con, "student");
            con.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void getVersion(Connection conn) {
        Statement statement = null;
        ResultSet result = null;
        try {
            statement = conn.createStatement();
            result = statement.executeQuery("SELECT @@VERSION as version ;");
            while (result.next()) {
                String version = result.getString("version");
                boolean flag = version.contains("Linux");
                if (flag) {
                    System.out.println("Linux");
                } else {
                    System.out.println("window");
                }

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

    public static void getColumns(Connection conn, String tableName) {
        ResultSet result = null;
        Statement statement = null;
        try {
            statement = conn.createStatement();
            String sql = "SELECT "
                    + " a.name AS table_name, "
                    + " b.name AS column_name, "
                    + " c.name AS type_name, "
                    + " b.max_length AS length, "
                    + " b.precision, "
                    + " b.scale, "
                    + " b.is_nullable, "
                    + " CONVERT(varchar(50),isnull( d.text, '')) as column_default, "
                    + " CONVERT(int ,CASE  "
                    + "  WHEN EXISTS (SELECT 1 FROM sys.objects WHERE type = 'PK' AND parent_object_id = b.object_id  "
                    + "  AND name IN ( SELECT name FROM sysindexes WHERE indid IN "
                    + " (SELECT indid FROM sysindexkeys WHERE id = b.object_id AND colid = b.column_id ))) THEN "
                    + " 1 ELSE 0 END) AS is_pk, "
                    + "  CONVERT(varchar(50),isnull(e.value,'')) AS description  "
                    + " FROM "
                    + "  sys.objects a "
                    + "  INNER JOIN sys.columns b ON b.object_id = a.object_id "
                    + "  INNER JOIN sys.types c ON c.system_type_id = b.system_type_id "
                    + "  LEFT JOIN syscomments d ON d.id = b.default_object_id "
                    + "  LEFT JOIN sys.extended_properties e ON e.major_id = b.object_id  "
                    + "  AND e.minor_id = b.column_id  "
                    + "WHERE "
                    + " A.name = '"
                    + tableName
                    + "'";
            System.out.println(sql);
            result = statement.executeQuery(sql);
            while (result.next()) {
                //字段注释
                String columComment = null == result.getString("description")
                        ? "" : result.getString("description");
                System.out.println("---------columComment--------" + columComment);
                Long length = result.getLong("length");
                if (length > 10000L) {
                    length = 0L;
                }
                System.out.println("---------length--------" + length);
                String defaultValue = null == result.getString("column_default")
                        ? "" : result.getString("column_default");
                System.out.println("---------defaultValue--------" + defaultValue);
                //默认是否为null
                Boolean isNullable = "1".equals(result.getString("is_nullable")) ? true : false;
                System.out.println("---------isNullable--------" + isNullable);
                //是否为主键
                Boolean isPk = "1".equals(result.getString("is_pk")) ? true : false;
                System.out.println("---------isPk--------" + isPk);
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

    public static void getCountNum(Connection conn, String tableName) {
        ResultSet result = null;
        Statement statement = null;
        try {
            statement = conn.createStatement();
            String sql = "select count(*) as total from " + tableName;
            result = statement.executeQuery(sql);
            while (result.next()) {
                //表数据量
                String total = String.valueOf(result.getLong("total"));
                System.out.println("===========================" + total);
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


    public static List<String> getTableNameList(Connection conn, String dbName, String schemaName) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<String> tableNameList = new ArrayList<>();
        String dbQuery = "SELECT * FROM information_schema.tables WHERE table_catalog = '" + dbName + "'";
        try {
            statement = conn.prepareStatement(dbQuery);
            rs = statement.executeQuery();
            while (rs.next()) {
                tableNameList.add(rs.getString("table_name"));
            }

            return tableNameList;
        } catch (SQLException e) {
            e.getMessage();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }


    public static List<String> getColumnNameList(Connection conn, String dbName, String schemaName, String tableName) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<String> columnList = new ArrayList<>();
        String dbQuery = "select * from " + tableName + " where 1=0";
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
        } catch (SQLException e) {
            e.getMessage();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
