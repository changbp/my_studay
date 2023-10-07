package com.cbp.test;

import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/9/19 11:26
 */
public class AtlasOracleTest {
    public static void main(String[] args) throws Exception {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.159:21000/"}, new String[]{"admin", "!QAZ2wsx3edc"});
//        File file = new File("D:\\rdbms\\40000-clickhouse_model.json");
//        String jsonStr = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
//        AtlasTypesDef atlasTypesDef = JSONObject.parseObject(jsonStr, AtlasTypesDef.class);
//        atlasClientV2.createAtlasTypeDefs(atlasTypesDef);
        // 创建instance
//        AtlasEntity.AtlasEntityWithExtInfo instance = createInstance(getConn());
//        atlasClientV2.createEntity(instance);
        // 创建db
//        atlasClientV2.deleteEntityByGuid("c4c9cb7b-dda2-4b48-b878-0bd26e7defa6");
//        List<String> databases = getDatabases(getConn());
//        if (databases != null) {
//            for (String database : databases) {
//                try {
//                    atlasClientV2.createEntity(createDb(database, "ee53eb19-9113-417c-8d0f-019dd9c35a60"));
//                } catch (AtlasServiceException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        atlasClientV2.deleteEntityByGuid("f4c0f641-b5cc-495a-8f23-c967686f54a");
//        atlasClientV2.deleteEntityByGuid("ecb7b428-6b86-4055-bccd-9f7708825ccb");
//        // 创建table
//        List<String> nameList = getTableNameList(getConn(), "db_2023");
//        if (nameList != null && nameList.size() > 0) {
//            nameList.forEach(tableName -> {
//                String qualifiedNameTable = "db_2023" + "." + tableName + "@192.168.110.101@clickhouse";
//                AtlasEntity.AtlasEntityWithExtInfo table = createTable(getConn(), tableName,
//                        qualifiedNameTable, "6e821c23-5901-4a33-8933-729d1bbfee80");
//                try {
//                    atlasClientV2.createEntity(table);
//                } catch (AtlasServiceException e) {
//                    e.printStackTrace();
//                }
//            });
//        }
        //  创建column
//        atlasClientV2.deleteEntityByGuid("184dea0b-e613-40d9-a06f-858011372dca");
//        atlasClientV2.deleteEntityByGuid("fbbd1cf4-bc0e-45b3-9c11-a6ddf4fb8fd0");

        List<String> columnNameList = getColumnNameList(getConn(), "db_2023", "test_table1");
        if (columnNameList != null && columnNameList.size() > 0) {
            columnNameList.forEach(columnName -> {
                String qualifiedNameColumnName =
                        "db_2023.test_table1" + "." + columnName + "@192.168.110.101@clickhouse";
                AtlasEntity.AtlasEntityWithExtInfo column = createColumn(getConn(), columnName,
                        qualifiedNameColumnName, "4554a0c1-e4a7-4edf-a390-a585e614c2b7");
                try {
                    atlasClientV2.createEntity(column);
                } catch (AtlasServiceException e) {
                    e.printStackTrace();
                }
            });
        }
    }


    public static Connection getConn() {
        // 创建数据库连接
        String url = "jdbc:clickhouse://192.168.110.101:8123/db_2023";
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

    public static List<String> getDatabases(Connection conn) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<String> tableNameList = new ArrayList<>();
        String dbQuery = "SELECT name " +
                "FROM system.databases " +
                "WHERE (uuid != '00000000-0000-0000-0000-000000000000') AND (name != 'system');";
        try {
            statement = conn.prepareStatement(dbQuery);
            rs = statement.executeQuery();
            while (rs.next()) {
                tableNameList.add(rs.getString(1));
            }
            return tableNameList;
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

    public static List<String> getTableNameList(Connection conn, String dbName) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<String> tableNameList = new ArrayList<>();
        String dbQuery = "select name from system.tables where database = '" + dbName + "'";
        try {
            statement = conn.prepareStatement(dbQuery);
            rs = statement.executeQuery();
            while (rs.next()) {
                tableNameList.add(rs.getString(1));
            }
            return tableNameList;
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

    public static AtlasEntity.AtlasEntityWithExtInfo createInstance(Connection conn) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("clickhouse_instance");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", "192.168.110.101@clickhouse");
        attributes.put("name", "clickhouse");
        attributes.put("rdbms_type", "clickhouse");
        Statement statement = null;
        ResultSet result = null;
        try {
            statement = conn.createStatement();
            result = statement.executeQuery("select value as version from system.build_options where name = 'SYSTEM';");
            while (result.next()) {
                //平台 linux 还是Windows
                String version = result.getString("version");
                boolean flag = version.contains("Linux");
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
        attributes.put("hostname", "192.168.110.101");
        attributes.put("port", "8123");
        attributes.put("owner", "default");
        attributes.put("contact_info", "测试ck创建instance");
        attributes.put("description", "测试ck创建instance");
        attributes.put("protocol", "jdbc");
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }

    private static AtlasEntity.AtlasEntityWithExtInfo createDb(String dbName, String guId) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("clickhouse_db");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", dbName + "@192.168.110.101@clickhouse");
        attributes.put("owner", "default");
        attributes.put("ownerType", "user");
        attributes.put("name", dbName);
        //查date
        attributes.put("emailAddress", "1@qq.com");
        attributes.put("createdBy", "default");
        attributes.put("createTime", new Date());
        attributes.put("updatedBy", "default");
        attributes.put("updateTime", new Date());
        //描述
        attributes.put("description", "测试ck创建db");
        HashMap<String, Object> instance = new HashMap<>(5);
        instance.put("guid", guId);
        instance.put("typeName", "clickhouse_instance");
        attributes.put("instance", instance);
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }

    private static AtlasEntity.AtlasEntityWithExtInfo createTable(Connection conn, String tableName,
                                                                  String qualifiedNameTable, String guId) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("clickhouse_table");
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
        db.put("typeName", "clickhouse_db");
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
            String sql = "select * from system.tables where database = '" + dbName + "' and name = '" + tableName + "'";
            result = statement.executeQuery(sql);
            while (result.next()) {
                attributes.put("type", "clickhouse_table");
                attributes.put("description", "");
                attributes.put("comment", "");
                String createDate = result.getString("metadata_modification_time");
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                try {
                    Date parse = simpleDateFormat.parse(createDate);
                    attributes.put("createTime", parse);
                } catch (ParseException e) {
                    System.out.println(e.getMessage());
                }
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
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static AtlasEntity.AtlasEntityWithExtInfo createColumn(Connection conn, String columnName,
                                                                   String qualifiedNameColumn, String guId) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("clickhouse_column");
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
            String sql = "select * from system.columns where database = '" + dbName + "' and table = '"
                    + tableName + "' and name = '" + columnName.split(":")[0] + "'";
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                //字段注释
                String columComment = null == resultSet.getString("comment")
                        ? "" : resultSet.getString("comment");
                attributes.put("comment", columComment);
                attributes.put("description", columComment);
                //长度限制 需要判空 默认传0
                Long length = resultSet.getLong("numeric_precision");
                if (length > 10000L) {
                    length = 0L;
                }
                attributes.put("length", length);
                String defaultValue = null == resultSet.getString("default_expression")
                        ? "" : resultSet.getString("default_expression");
                attributes.put("default_value", defaultValue);
                //是否为主键
                Boolean isPk = "1".equals(resultSet.getString("is_in_primary_key"));
                attributes.put("isPrimaryKey", isPk);
                //默认是否为null
                if (isPk) {
                    attributes.put("isNullable", false);
                } else {
                    attributes.put("isNullable", true);
                }

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
        table.put("typeName", "clickhouse_table");
        attributes.put("table", table);
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }


}
