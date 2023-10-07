package com.cbp.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasTypesDef;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/9/19 11:26
 */
public class AtlasPgTest {
    public static void main(String[] args) throws Exception {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.159:21000/"}, new String[]{"admin", "!QAZ2wsx3edc"});
//        File file = new File("D:\\rdbms\\20000-postgresql_model.json");
//        String jsonStr = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
//        AtlasTypesDef atlasTypesDef = JSONObject.parseObject(jsonStr, AtlasTypesDef.class);
//        atlasClientV2.createAtlasTypeDefs(atlasTypesDef);
//        // 创建instance
//        AtlasEntity.AtlasEntityWithExtInfo instance = createInstance(getConn());
//        atlasClientV2.createEntity(instance);
        // 创建db
//        AtlasEntity.AtlasEntityWithExtInfo database = createDb("db_2023", "c76d14c7-864a-484b-a571-8c8225d86899");
////        atlasClientV2.deleteEntityByGuid("f27fde31-247c-4081-9da5-276423c492a1");
//        atlasClientV2.createEntity(database);
        // 创建table
//        List<String> nameList = getTableNameList(getConn(), "db_2023");
//        if (nameList != null && nameList.size() > 0) {
//            nameList.forEach(tableName -> {
//                String qualifiedNameTable = "db_2023" + "." + tableName + "@192.168.110.101@postgresql";
//                AtlasEntity.AtlasEntityWithExtInfo table = createTable(getConn(), tableName,
//                        qualifiedNameTable,"bb612052-bc52-435a-9e3a-0c9189bc2c0a");
//                try {
//                    atlasClientV2.createEntity(table);
//                } catch (AtlasServiceException e) {
//                    e.printStackTrace();
//                }
//            });
//        }
        // 创建column
//        atlasClientV2.deleteEntityByGuid("184dea0b-e613-40d9-a06f-858011372dca");
//        atlasClientV2.deleteEntityByGuid("fbbd1cf4-bc0e-45b3-9c11-a6ddf4fb8fd0");
//
        List<String> columnNameList = getColumnNameList(getConn(), "db_2023", "student");
        if (columnNameList != null && columnNameList.size() > 0) {
            columnNameList.forEach(columnName -> {
                String qualifiedNameColumnName =
                        "db_2023.student" + "." + columnName + "@192.168.110.101@postgresql";
                AtlasEntity.AtlasEntityWithExtInfo column = createColumn(getConn(), columnName,
                        qualifiedNameColumnName, "2ebd355a-af63-4fc3-915d-372dd2db3b74");
                try {
                    atlasClientV2.createEntity(column);
                } catch (AtlasServiceException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private static AtlasEntity.AtlasEntityWithExtInfo createColumn(Connection conn, String columnName,
                                                                   String qualifiedNameColumn, String guId) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("postgresql_column");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", qualifiedNameColumn);
        attributes.put("createdBy", "admin");
        attributes.put("name", columnName.split(":")[0]);
        String[] qualifiedNameColumnSplit = qualifiedNameColumn.split("\\.");
        String dbName = qualifiedNameColumnSplit[0];
        String tableName = qualifiedNameColumnSplit[1];
        ResultSet resultSet = null;
        Statement statement = null;
        try {
            statement = conn.createStatement();
            String sql = getSql(tableName, dbName, columnName.split(":")[0]);
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                //字段注释
                String columnComment = resultSet.getString("detext");
                attributes.put("comment", columnComment);
                attributes.put("description", columnComment);
                //长度限制 需要判空 默认传0
                long length = resultSet.getLong("length");
                if (length > 10000L) {
                    length = 0L;
                }
                attributes.put("length", length);
                String defaultValue = resultSet.getString("defaultval");
                attributes.put("default_value", defaultValue);
                //默认是否为null
                Boolean isNullable = "0".equals(resultSet.getString("is_nullable"));
                attributes.put("isNullable", isNullable);
                //是否为主键
                Boolean isPk = "1".equals(resultSet.getString("ispk"));
                attributes.put("isPrimaryKey", isPk);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        attributes.put("owner", "admin");
        attributes.put("data_type", columnName.split(":")[1]);
        attributes.put("contact_info", "");
        HashMap<String, Object> table = new HashMap<>(5);
        table.put("guid", guId);
        table.put("typeName", "postgresql_table");
        attributes.put("table", table);
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }

    /**
     * 拼接sql(编码规则，一个方法不允许超过80行)
     */
    public static String getSql(String tableName, String dbName, String columnName) {
        String sql = "SELECT "
                + " ordinal_position AS Colorder, "
                + " COLUMN_NAME AS ColumnName, "
                + " data_type AS TypeName, "
                + " COALESCE ( character_maximum_length, numeric_precision,- 1 ) AS LENGTH, "
                + " numeric_scale AS SCALE, "
                + "CASE is_nullable  WHEN 'NO' THEN  1 ELSE 0   END AS is_nullable, "
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
                + "and COLUMN_NAME = '"
                + columnName
                + "'"
                + "ORDER BY "
                + " ordinal_position ASC";
        return sql;
    }

    public static List<String> getColumnNameList(Connection conn, String dbName, String tableName) {
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
            e.printStackTrace();
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

    private static AtlasEntity.AtlasEntityWithExtInfo createTable(Connection conn, String tableName,
                                                                  String qualifiedNameTable, String guId) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("postgresql_table");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", qualifiedNameTable);
        setTable(conn, "db_2023", tableName, attributes);
        //空的查元数据
        attributes.put("createdBy", "root");
        attributes.put("dbName", "db_2023");
        attributes.put("displayName", "");
        //外键
        attributes.put("name", tableName);
        attributes.put("owner", "root");
        ResultSet resultSet = null;
        Statement statement = null;
        try {
            statement = conn.createStatement();
            String sql = "select count(1) as total from " + tableName;
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                //表数据量
                attributes.put("contact_info", String.valueOf(resultSet.getLong("total")));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        HashMap<String, Object> db = new HashMap<>(5);
        db.put("guid", guId);
        db.put("typeName", "postgresql_db");
        attributes.put("db", db);
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }

    private static void setTable(Connection conn, String dbName, String tableName, HashMap<String, Object> attributes) {
        Statement statement = null;
        ResultSet result = null;
        try {
            statement = conn.createStatement();
            String sql = "select table_type from information_schema.tables where table_catalog ='" + dbName
                    + "' and table_name = '" + tableName + "'";
            result = statement.executeQuery(sql);
            while (result.next()) {
                String tableType = result.getString("table_type");
                if ("VIEW".equals(tableType)) {
                    attributes.put("ddl", tableType);
                    attributes.put("type", "postgresql_view");
                } else {
                    attributes.put("ddl", tableType);
                    attributes.put("type", "postgresql_table");
                }
            }
            result = null;
            String tableDesSql = "SELECT a.relname AS TABLE_NAME,b.description "
                    + "FROM pg_class a "
                    + "LEFT JOIN ( SELECT * FROM pg_description WHERE objsubid = 0 ) b ON A.oid = b.objoid "
                    + "WHERE "
                    + "A.relname = ( SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema "
                    + "= 'public' AND table_catalog = '"
                    + dbName
                    + "' AND table_name = '"
                    + tableName
                    + "' )";
            result = statement.executeQuery(tableDesSql);
            while (result.next()) {
                String description = result.getString("description");
                attributes.put("description", description);
                attributes.put("comment", description);
                attributes.put("createTime", 0L);
            }
            result = null;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (result != null) {
                    result.close();
                }
                if (statement != null) {
                    statement.close();
                }
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static AtlasEntity.AtlasEntityWithExtInfo createDb(String dbName, String guId) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("postgresql_db");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", "db_2023@192.168.110.101@postgresql");
        attributes.put("owner", "psotgres");
        attributes.put("ownerType", "user");
        attributes.put("name", dbName);
        //查date
        attributes.put("emailAddress", "1@qq.com");
        attributes.put("createdBy", "psotgres");
        attributes.put("createTime", new Date());
        attributes.put("updatedBy", "psotgres");
        attributes.put("updateTime", new Date());
        //描述
        attributes.put("description", "测试pg创建db");
        HashMap<String, Object> instance = new HashMap<>(5);
        instance.put("guid", guId);
        instance.put("typeName", "postgresql_instance");
        attributes.put("instance", instance);
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }

    public static List<String> getTableNameList(Connection conn, String dbName) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<String> tableNameList = new ArrayList<>();
        String dbQuery = "SELECT\n" +
                "\ttable_name\n" +
                "FROM\n" +
                "\tinformation_schema.tables \n" +
                "WHERE\n" +
                "\ttable_catalog = '" + dbName + "' and table_schema = 'public'";
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
                rs.close();
                statement.close();
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

    public static AtlasEntity.AtlasEntityWithExtInfo createInstance(Connection conn) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("postgresql_instance");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", "192.168.110.101@postgresql");
        attributes.put("name", "postgresql");
        attributes.put("rdbms_type", "PostgreSql");
        Statement statement = null;
        ResultSet result = null;
        try {
            statement = conn.createStatement();
            result = statement.executeQuery("SELECT split_part(version(), ',', 1) as version");
            while (result.next()) {
                //平台 linux 还是Windows
                String[] versions = result.getString("version").split("-");
                boolean flag = Arrays.toString(versions).contains("linux");
                if (flag) {
                    attributes.put("platform", "Linux");
                } else {
                    attributes.put("platform", "Window");
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                result.close();
                statement.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        attributes.put("hostname", "192.168.110.101");
        attributes.put("port", "5432");
        attributes.put("owner", "postgres");
        attributes.put("contact_info", "测试pg创建instance");
        attributes.put("description", "测试pg创建instance");
        attributes.put("protocol", "jdbc");
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }
}
