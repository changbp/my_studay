package com.cbp.test;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/9/22 14:26
 */
public class PgTest {
    private static ResultSet result;

    private static Statement statement;

    public static void main(String[] args) {
//        List<String> dataBaseNameList = getDataBaseNameList(getConn());
//        System.out.println(dataBaseNameList);
//        System.out.println("=========================databases==========================");
//        if (dataBaseNameList != null) {
//            dataBaseNameList.forEach(dataBase -> {
//                List<String> tableNameList = getTableNameList(getConn(), dataBase);
//                System.out.println(tableNameList);
//                System.out.println("=========================tableNames==========================" + dataBase);
//            });
//        }
        HashMap<String, Object> attributes = new HashMap<>();
        setTable("db_2023", "public", "student", attributes);
        System.out.println(attributes);
    }

    private static void setTable(String dbName, String schema, String tableName, HashMap<String, Object> attributes) {
        Connection conn = getConn();
        try {
            String tableDesSql = "SELECT"
                    + " p1.relname,"
                    + " p1.relkind,"
                    + " p2.description "
                    + "FROM"
                    + " pg_class p1"
                    + " LEFT JOIN ( SELECT * FROM pg_description WHERE objsubid = 0 ) p2 ON p1.oid = p2.objoid"
                    + " LEFT JOIN pg_namespace p3 ON p1.relnamespace = p3.oid "
                    + "WHERE"
                    + " p3.nspname = '" + schema + "' "
                    + " AND p1.relname = '" + tableName + "'";
            statement = conn.createStatement();
            result = statement.executeQuery(tableDesSql);
            while (result.next()) {
                String description = result.getString("description");
                attributes.put("description", description);
                attributes.put("comment", description);
                String relKind = result.getString("relkind");
                if ("r".equals(relKind)) {
                    attributes.put("type", "postgresql_table");
                }else{
                    attributes.put("type", "postgresql_view");
                }
            }
        } catch (SQLException e) {
            e.getMessage();
        } finally {
            try {
                result.close();
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            String tableDdlSql = getDdlSql(schema, tableName);
            statement = conn.createStatement();
            result = statement.executeQuery(tableDdlSql);
            while (result.next()) {
                attributes.put("ddl", result.getString(1));
            }
        } catch (SQLException e) {
            e.getMessage();
        } finally {
            try {
                result.close();
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    public static String getDdlSql(String schemaName, String tableName) {
        return "SELECT array_to_string( ARRAY ("
                + "  SELECT ret  FROM"
                + "      ("
                + "        SELECT"
                + "      'CREATE TABLE \"" + schemaName + "\"." + tableName + " (' || array_to_string(ARRAY ("
                + "          SELECT SQL "
                + "          FROM"
                + "              ("
                + "            ( "
                + "                SELECT array_to_string(ARRAY ("
                + "                SELECT A.attname || ' ' || concat_ws ( '', T.typname, SUBSTRING ( format_type ( A.atttypid, A.atttypmod ) FROM '\\(.*\\)' ) ) || ' ' ||"
                + "                CASE A.attnotnull  WHEN 't' THEN 'NOT NULL' ELSE '' END || ' ' ||"
                + "                CASE WHEN D.adbin IS NOT NULL THEN ' DEFAULT ' || pg_get_expr ( D.adbin, A.attrelid ) ELSE'' END "
                + "                FROM"
                + "              pg_attribute"
                + "              A LEFT JOIN pg_description b ON A.attrelid = b.objoid "
                + "              AND A.attnum = b.objsubid"
                + "              LEFT JOIN pg_attrdef D ON A.attrelid = D.adrelid "
                + "              AND A.attnum = D.adnum,"
                + "              pg_type T "
                + "                WHERE"
                + "              A.attstattarget =- 1 "
                + "              AND A.attrelid = '\"" + schemaName + "\"." + tableName + "' :: REGCLASS :: OID "
                + "              AND A.attnum > 0 "
                + "              AND NOT A.attisdropped "
                + "              AND A.atttypid = T.OID "
                + "                ORDER BY A.attnum  ),',' || CHR( 10 )  )  as SQL, 1 seri "
                + "            ) "
                + "            UNION"
                + "            ( "
                + "                SELECT 'CONSTRAINT ' || conname || ' ' || pg_get_constraintdef ( OID ) as SQL, 2 seri "
                + "                FROM pg_constraint T "
                + "                WHERE conrelid = '\"" + schemaName + "\"." + tableName + "' :: REGCLASS :: OID "
                + "                ORDER BY contype DESC "
                + "            ) "
                + "            ORDER BY seri  "
                + "              ) T "
                + "            ),',' || CHR( 10 )  ) || ')' AS ret, 1 AS orderby "
                + "        UNION  "
                + "          SELECT array_to_string(ARRAY ( "
                + "              SELECT pg_get_indexdef ( indexrelid ) "
                + "              FROM pg_index "
                + "              WHERE indrelid = '\"" + schemaName + "\"." + tableName + "' :: REGCLASS :: OID "
                + "              AND indisprimary = 'f' "
                + "              AND indisunique = 'f' "
                + "            ),';' || CHR( 10 ) ) AS ret,2 AS orderby "
                + "        UNION  "
                + "          SELECT array_to_string(ARRAY ("
                + "            SELECT"
                + "                'COMMENT ON COLUMN ' || '\"" + schemaName + "\"." + tableName + ".' || A.attname || ' IS ''' || b.description || '''' "
                + "            FROM pg_attribute A "
                + "            LEFT JOIN pg_description b ON A.attrelid = b.objoid "
                + "                AND A.attnum = b.objsubid "
                + "            WHERE"
                + "                A.attstattarget =- 1 "
                + "                AND A.attrelid = '\"" + schemaName + "\"." + tableName + "' :: REGCLASS :: OID "
                + "                AND b.description IS NOT NULL "
                + "            ORDER BY"
                + "                A.attnum "
                + "            ), ';' || CHR( 10 )  ) AS ret,3 AS orderby "
                + "      ORDER BY orderby "
                + "      ) results "
                + "  ), ';' || CHR( 10 ) || CHR( 13 ) "
                + ")";
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
        // 创建数据库连接 db_2023
        String url = "jdbc:postgresql://192.168.110.101:5432/db_2023";
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
