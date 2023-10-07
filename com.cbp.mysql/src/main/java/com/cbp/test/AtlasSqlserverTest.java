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
public class AtlasSqlserverTest {
    public static void main(String[] args) throws Exception {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.159:21000/"}, new String[]{"admin", "!QAZ2wsx3edc"});
//        File file = new File("D:\\rdbms\\30000-sqlserver_model.json");
//        String jsonStr = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
//        AtlasTypesDef atlasTypesDef = JSONObject.parseObject(jsonStr, AtlasTypesDef.class);
//        atlasClientV2.createAtlasTypeDefs(atlasTypesDef);
//        // 创建instance
//        AtlasEntity.AtlasEntityWithExtInfo instance = createInstance(getConn());
//        atlasClientV2.createEntity(instance);
        // 创建db
//        AtlasEntity.AtlasEntityWithExtInfo database = createDb("students",
//                "41d94ab2-0ff1-424a-8df1-9a30b10dc024");
////        atlasClientV2.deleteEntityByGuid("f27fde31-247c-4081-9da5-276423c492a1");
//        atlasClientV2.createEntity(database);
        // 创建table
//        List<String> nameList = getTableNameList(getConn(), "students");
//        if (nameList != null && nameList.size() > 0) {
//            nameList.forEach(tableName -> {
//                String qualifiedNameTable = "students" + "." + tableName + "@192.168.110.101@sqlserver";
//                AtlasEntity.AtlasEntityWithExtInfo table = createTable(getConn(), tableName,
//                        qualifiedNameTable, "63ca2e0a-6ee0-456b-82bb-4beb58870e67");
//                try {
//                    atlasClientV2.createEntity(table);
//                } catch (AtlasServiceException e) {
//                    e.printStackTrace();
//                }
//            });
//        }

//        atlasClientV2.deleteEntityByGuid("eb838c08-401d-4b96-ac13-0be78fa5a9e6");
//        atlasClientV2.deleteEntityByGuid("21c9b84c-1a2f-459f-95d2-1a77532fd072");
//        atlasClientV2.deleteEntityByGuid("e850df77-c675-41ed-9fb4-188e8dce7c1a");
//        atlasClientV2.deleteEntityByGuid("0c6498f1-c69b-4330-affe-d476cc38d6a5");
//        atlasClientV2.deleteEntityByGuid("7ac3e819-a3b3-45a1-845f-51f4db090ece");
//        atlasClientV2.deleteEntityByGuid("68630722-5d88-48c5-b427-9e494f57be08");
        // 创建column
        List<String> columnNameList = getColumnNameList(getConn(), "students", "student");
        if (columnNameList != null && columnNameList.size() > 0) {
            columnNameList.forEach(columnName -> {
                String qualifiedNameColumnName =
                        "students.student" + "." + columnName + "@192.168.110.101@sqlserver";
                AtlasEntity.AtlasEntityWithExtInfo column = createColumn(getConn(), columnName,
                        qualifiedNameColumnName, "b0155d92-2a7c-4ec2-8927-40d01dee54df");
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
        atlasEntity.setTypeName("sqlserver_column");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", qualifiedNameColumn);
        attributes.put("createdBy", "sa");
        attributes.put("name", columnName.split(":")[0]);
        String[] qualifiedNameColumnSplit = qualifiedNameColumn.split("\\.");
        String dbName = qualifiedNameColumnSplit[0];
        String tableName = qualifiedNameColumnSplit[1];
        ResultSet resultSet = null;
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
                    + "' and b.name = '"
                    + columnName.split(":")[0]
                    + "'";
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                //字段注释
                String columComment = null == resultSet.getString("description")
                        ? "" : resultSet.getString("description");
                attributes.put("comment", columComment);
                attributes.put("description", columComment);
                //长度限制 需要判空 默认传0
                Long length = resultSet.getLong("length");
                if (length > 10000L) {
                    length = 0L;
                }
                attributes.put("length", length);
                String defaultValue = null == resultSet.getString("column_default")
                        ? "" : resultSet.getString("column_default");
                attributes.put("default_value", defaultValue);
                //默认是否为null
                Boolean isNullable = "1".equals(resultSet.getString("is_nullable"));
                attributes.put("isNullable", isNullable);
                //是否为主键
                Boolean isPk = "1".equals(resultSet.getString("is_pk"));
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
        attributes.put("owner", "sa");
        attributes.put("data_type", columnName.split(":")[1]);
        attributes.put("contact_info", "");
        HashMap<String, Object> table = new HashMap<>(5);
        table.put("guid", guId);
        table.put("typeName", "sqlserver_table");
        attributes.put("table", table);
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
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
        atlasEntity.setTypeName("sqlserver_table");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", qualifiedNameTable);
        setTable(conn, "students", tableName, attributes);
        //空的查元数据
        attributes.put("createdBy", "sa");
        attributes.put("dbName", "students");
        attributes.put("displayName", "students");
        //外键
        attributes.put("name", tableName);
        attributes.put("owner", "sa");
        ResultSet resultSet = null;
        Statement statement = null;
        try {
            statement = conn.createStatement();
            String sql = "select count(*) as total from " + tableName;
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
        db.put("typeName", "sqlserver_db");
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
            String sql = "SELECT "
                    + " a.name, "
                    + " a.type, "
                    + " a.create_date, "
                    + " convert(varchar(100),b.VALUE) AS description  "
                    + "FROM "
                    + " sys.objects a "
                    + " LEFT JOIN sys.extended_properties b ON a.object_id = b.major_id  "
                    + " AND b.minor_id = 0  "
                    + "WHERE "
                    + " a.name = '"
                    + tableName
                    + "'";
            result = statement.executeQuery(sql);
            while (result.next()) {
                String tableType = result.getString("type");
                if ("V".equals(tableType)) {
                    attributes.put("ddl", tableType);
                    attributes.put("type", "sqlserver_view");
                } else {
                    attributes.put("ddl", tableType);
                    attributes.put("type", "sqlserver_table");
                }
                String description = result.getString("description");
                attributes.put("description", description);
                attributes.put("comment", description);
                String createDate = result.getString("create_date");
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                try {
                    Date parse = simpleDateFormat.parse(createDate);
                    attributes.put("createTime", parse);
                } catch (ParseException e) {
                    e.printStackTrace();
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

    private static AtlasEntity.AtlasEntityWithExtInfo createDb(String dbName, String guId) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("sqlserver_db");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", "students@192.168.110.101@sqlserver");
        attributes.put("owner", "sa");
        attributes.put("ownerType", "user");
        attributes.put("name", dbName);
        //查date
        attributes.put("emailAddress", "1@qq.com");
        attributes.put("createdBy", "sa");
        attributes.put("createTime", new Date());
        attributes.put("updatedBy", "sa");
        attributes.put("updateTime", new Date());
        //描述
        attributes.put("description", "测试sqlserver创建db");
        HashMap<String, Object> instance = new HashMap<>(5);
        instance.put("guid", guId);
        instance.put("typeName", "sqlserver_instance");
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
        String dbQuery = "SELECT * FROM information_schema.tables WHERE table_catalog = '" + dbName + "'";
        try {
            statement = conn.prepareStatement(dbQuery);
            rs = statement.executeQuery();
            while (rs.next()) {
                tableNameList.add(rs.getString("table_name"));
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
        String url = "jdbc:sqlserver://hadoop1:1433;database=students";
        String username = "sa";
        String password = "1234Qwer";
        Connection conn = null;
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            conn = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo createInstance(Connection conn) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("sqlserver_instance");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", "192.168.110.101@sqlserver");
        attributes.put("name", "sqlserver");
        attributes.put("rdbms_type", "SqlServer");
        Statement statement = null;
        ResultSet result = null;
        try {
            statement = conn.createStatement();
            result = statement.executeQuery("SELECT @@VERSION as version ;");
            while (result.next()) {
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
                result.close();
                statement.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        attributes.put("hostname", "192.168.110.101");
        attributes.put("port", "1433");
        attributes.put("owner", "sa");
        attributes.put("contact_info", "测试sqlserver创建instance");
        attributes.put("description", "测试sqlserver创建instance");
        attributes.put("protocol", "jdbc");
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }
}
