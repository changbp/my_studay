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
//        File file = new File("D:\\rdbms\\50000-oracle_model.json");
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
//                    atlasClientV2.createEntity(createDb(database, "2fda6538-2b80-4e65-bb55-ba3fde5ca104"));
//                } catch (AtlasServiceException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        atlasClientV2.deleteEntityByGuid("f4c0f641-b5cc-495a-8f23-c967686f54a");
        atlasClientV2.deleteEntityByGuid("3c9c7d3e-2350-40c4-8331-058eafd70ba1");
//        // 创建table
        List<String> nameList = getTableNameList(getConn(), "SYSTEM");
        if (nameList != null && nameList.size() > 0) {
            nameList.forEach(tableName -> {
                String qualifiedNameTable = "SYSTEM" + "." + tableName + "@192.168.110.101@oracle";
                AtlasEntity.AtlasEntityWithExtInfo table = createTable(getConn(), tableName,
                        qualifiedNameTable, "3c6ac57b-985a-4886-821e-4cc05d4525f0");
                try {
                    atlasClientV2.createEntity(table);
                } catch (AtlasServiceException e) {
                    e.printStackTrace();
                }
            });
        }
        //  创建column
//        atlasClientV2.deleteEntityByGuid("184dea0b-e613-40d9-a06f-858011372dca");
//        atlasClientV2.deleteEntityByGuid("fbbd1cf4-bc0e-45b3-9c11-a6ddf4fb8fd0");
//
//        List<String> columnNameList = getColumnNameList(getConn(), "system", "student");
//        if (columnNameList != null && columnNameList.size() > 0) {
//            columnNameList.forEach(columnName -> {
//                String qualifiedNameColumnName =
//                        "system.student" + "." + columnName + "@192.168.110.101@oracle";
//                AtlasEntity.AtlasEntityWithExtInfo column = createColumn(getConn(), columnName,
//                        qualifiedNameColumnName, "4554a0c1-e4a7-4edf-a390-a585e614c2b7");
//                try {
//                    atlasClientV2.createEntity(column);
//                } catch (AtlasServiceException e) {
//                    e.printStackTrace();
//                }
//            });
//        }
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

    public static List<String> getDatabases(Connection conn) {
        PreparedStatement statement = null;
        ResultSet rs = null;
        List<String> tableNameList = new ArrayList<>();
        String dbQuery = "select distinct owner from all_objects where owner = 'SYSTEM'";
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
        String dbQuery = "select table_name from dba_tables where owner = '" + dbName + "' and TABLE_NAME ='student'";
        try {
            statement = conn.prepareStatement(dbQuery);
            rs = statement.executeQuery();
            while (rs.next()) {
                tableNameList.add(rs.getString(1));
            }
            return tableNameList;
        } catch (SQLException e) {
            dbQuery = "select table_name from user_tables";
            try {
                statement = conn.prepareStatement(dbQuery);
                rs = statement.executeQuery();
                tableNameList = new ArrayList<>();
                while (rs.next()) {
                    tableNameList.add(rs.getString(1));
                }
            } catch (SQLException e1) {
                throw new RuntimeException(e1);
            }
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
        String dbQuery = "select * from \"" + dbName + "\".\"" + tableName + "\" where 1=0";
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
        atlasEntity.setTypeName("oracle_instance");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", "192.168.110.101@oracle");
        attributes.put("name", "oracle");
        attributes.put("rdbms_type", "oracle");
        Statement statement = null;
        ResultSet result = null;
        try {
            statement = conn.createStatement();
            result = statement.executeQuery("SELECT PLATFORM_NAME FROM V$DATABASE");
            while (result.next()) {
                //平台 linux 还是Windows
                String version = result.getString(1);
                boolean flag = version.toLowerCase().contains("linux");
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
        attributes.put("port", "1521");
        attributes.put("owner", "system");
        attributes.put("contact_info", "测试oracle创建instance");
        attributes.put("description", "测试oracle创建instance");
        attributes.put("protocol", "jdbc");
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }

    private static AtlasEntity.AtlasEntityWithExtInfo createDb(String dbName, String guId) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("oracle_db");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", dbName + "@192.168.110.101@oracle");
        attributes.put("owner", "system");
        attributes.put("ownerType", "user");
        attributes.put("name", dbName);
        //查date
        attributes.put("emailAddress", "1@qq.com");
        attributes.put("createdBy", "default");
        attributes.put("createTime", new Date());
        attributes.put("updatedBy", "default");
        attributes.put("updateTime", new Date());
        //描述
        attributes.put("description", "测试oracle创建db");
        HashMap<String, Object> instance = new HashMap<>(5);
        instance.put("guid", guId);
        instance.put("typeName", "oracle_instance");
        attributes.put("instance", instance);
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }

    private static AtlasEntity.AtlasEntityWithExtInfo createTable(Connection conn, String tableName,
                                                                  String qualifiedNameTable, String guId) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("oracle_table");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", qualifiedNameTable);
        String dbName = qualifiedNameTable.split("\\.")[0];
        setTable(conn, "system", tableName, attributes);
        //空的查元数据
        attributes.put("createdBy", "root");
        attributes.put("dbName", "system");
        attributes.put("displayName", "");
        //外键
        attributes.put("name", tableName);
        attributes.put("owner", "root");
        ResultSet resultSet = null;
        Statement statement = null;
        try {
            statement = conn.createStatement();
            String sql = "select count(1) as TOTAL from " + dbName + "." + tableName + "";
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
        db.put("typeName", "oracle_db");
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
//            statement = conn.createStatement();
//            String sql = "select dbms_metadata.get_ddl('TABLE','" + tableName + "','" + dbName + "') from dual";
//            result = statement.executeQuery(sql);
//            while (result.next()) {
//                if (result.getString(1) != null) {
//                    attributes.put("ddl", result.getString(1));
//                } else {
//                    attributes.put("ddl", "");
//                }
//            }
            //查询表注释
            setTableCommAndType(conn, tableName, attributes, dbName);
            //查询表创建时间
            String sql = "select created from dba_objects where object_name = '" + tableName + "' AND owner='" + dbName + "'";
            result.close();
            statement.close();
            statement = conn.createStatement();
            result = statement.executeQuery(sql);
            while (result.next()) {
                if (result.getDate(1) != null) {
                    attributes.put("createTime", result.getDate(1).getTime());
                } else {
                    attributes.put("createTime", 0L);
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
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void setTableCommAndType(Connection conn, String tableName, HashMap<String, Object> attributes, String dbName) {
        Statement statement = null;
        ResultSet result = null;
        String sql;
        if (dbName.equals("system")) {
            sql = "select COMMENTS from user_tab_comments where TABLE_NAME='" + tableName + "'";
            try {
                statement = conn.createStatement();
                result = statement.executeQuery(sql);
                while (result.next()) {
                    if (result.getString(1) != null) {
                        attributes.put("description", result.getString(1));
                        attributes.put("comment", result.getString(1));
                    } else {
                        attributes.put("description", "");
                        attributes.put("comment", "");
                    }
                }
                statement.close();
                result.close();
                //查询表类型
                sql = "select TABLE_TYPE from user_tab_comments where TABLE_NAME='" + tableName + "'";
                statement = conn.createStatement();
                result = statement.executeQuery(sql);
                while (result.next()) {
                    if (result.getString(1) != null) {
                        attributes.put("type", result.getString(1));
                    } else {
                        attributes.put("type", "Oracle_table");
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
        } else {
            sql = "select COMMENTS from dba_tab_comments where OWNER='"
                    + dbName + "' AND TABLE_NAME='" + tableName + "'";
            try {
                statement = conn.createStatement();
                result = statement.executeQuery(sql);
                while (result.next()) {
                    if (result.getString(1) != null) {
                        attributes.put("description", result.getString(1));
                        attributes.put("comment", result.getString(1));
                    } else {
                        attributes.put("description", "");
                        attributes.put("comment", "");
                    }
                }
                result.close();
                statement.close();
                //查询表类型
                sql = "select TABLE_TYPE from dba_tab_comments where OWNER='"
                        + dbName + "' AND TABLE_NAME='" + tableName + "'";
                statement = conn.createStatement();
                result = statement.executeQuery(sql);
                while (result.next()) {
                    if (result.getString(1) != null) {
                        attributes.put("type", result.getString(1));
                    } else {
                        attributes.put("type", "Oracle_table");
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
    }

    private static AtlasEntity.AtlasEntityWithExtInfo createColumn(Connection conn, String columnName,
                                                                   String qualifiedNameColumn, String guId) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("oracle_column");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", qualifiedNameColumn);
        attributes.put("createdBy", "root");
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
        table.put("typeName", "oracle_table");
        attributes.put("table", table);
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }


}
