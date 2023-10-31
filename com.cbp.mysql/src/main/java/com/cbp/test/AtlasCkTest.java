package com.cbp.test;

import com.alibaba.fastjson.JSONObject;
import com.unicom.udme.entity.atlas.req.AtlasFilterSearchReq;
import com.unicom.udme.entity.atlas.result.CommandResult;
import com.unicom.udme.entity.atlas.result.EntityMutationResponse;
import com.unicom.udme.entity.atlas.search.AtlasEntityWithExtInfo;
import jdk.nashorn.internal.ir.CallNode;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.elasticsearch.common.collect.Tuple;

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
public class AtlasCkTest {
    public static void main(String[] args) throws Exception {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.159:21000/"}, new String[]{"admin", "!QAZ2wsx3edc"});
//        File file = new File("D:\\rdbms\\40000-clickhouse_model.json");
//        String jsonStr = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
//        AtlasTypesDef atlasTypesDef = JSONObject.parseObject(jsonStr, AtlasTypesDef.class);
//        atlasClientV2.createAtlasTypeDefs(atlasTypesDef);
//        //拼接服务名称 创建instance
//        String qualifiedNameInstance = "192.168.110.101" + "@clickhouse";
////        AtlasEntity.AtlasEntityWithExtInfo instance = createInstance(getConn(), qualifiedNameInstance);
////        atlasClientV2.createEntity(instance);
//        //获取服务的 guid
//        String typeName = "clickhouse_cluster";
//        Tuple<String, String> guidAndQualifiedName = getEntity(atlasClientV2, typeName, qualifiedNameInstance);
//        System.out.println("=====================" + guidAndQualifiedName);
//        // 创建db
//        List<String> databases = getDatabases(getConn());
//        if (databases != null) {
//            for (String database : databases) {
//                try {
//                    String qualifiedNameDb = database + "@192.168.110.101@clickhouse";
//                    AtlasEntity.AtlasEntityWithExtInfo db = createDb(getConn(), database, qualifiedNameDb, guidAndQualifiedName);
//                    atlasClientV2.createEntity(db);
//                } catch (AtlasServiceException e) {
//                    e.printStackTrace();
//                }
//            }
//        }

//        // 创建table
        //获取db的 guid
//        String typeDbName = "clickhouse_db";
//        Tuple<String, String> guidAndQualifiedDbName = getEntity(atlasClientV2, typeDbName, "db_2023@192.168.110.101@clickhouse");
//        List<String> nameList = getTableNameList(getConn(), "db_2023");
//        if (nameList != null && nameList.size() > 0) {
//            nameList.forEach(tableName -> {
//                String qualifiedNameTable = "db_2023" + "." + tableName + "@192.168.110.101@clickhouse";
//                AtlasEntity.AtlasEntityWithExtInfo table = createTable(getConn(), tableName,
//                        qualifiedNameTable, guidAndQualifiedDbName);
//                try {
//                    atlasClientV2.createEntity(table);
//                } catch (AtlasServiceException e) {
//                    e.printStackTrace();
//                }
//            });
//        }
//          创建column
        String typeDbName = "clickhouse_table";
        Tuple<String, String> guidAndQualifiedTableName = getEntity(atlasClientV2, typeDbName, "db_2023.test_table1@192.168.110.101@clickhouse");
        List<String> columnNameList = getColumnNameList(getConn(), "db_2023", "test_table1");
        if (columnNameList != null && columnNameList.size() > 0) {
            columnNameList.forEach(columnName -> {
                String qualifiedNameColumnName =
                        "db_2023.test_table1" + "." + columnName + "@192.168.110.101@clickhouse";
                AtlasEntity.AtlasEntityWithExtInfo column = createColumn(getConn(), columnName,
                        qualifiedNameColumnName, guidAndQualifiedTableName);
                try {
                    atlasClientV2.createEntity(column);
                } catch (AtlasServiceException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public static AtlasEntity.AtlasEntityWithExtInfo createOrUpdateDbDdl(Connection conn, String qualifiedNameDbDdl,
                                                                         Tuple<String, String> guidAndQualifiedNameDb,
                                                                         String dbName) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("clickhouse_db_ddl");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", qualifiedNameDbDdl);
        attributes.put("owner", "root");
        attributes.put("name", dbName + "_db_ddl");
        ResultSet resultSet = null;
        Statement statement = null;
        try {
            String sql = "select * from system.databases where name = '" + dbName + "'";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                String engine = resultSet.getString("engine");
                attributes.put("text", "CREATE DATABASE " + dbName + " ENGINE = " + engine);
                attributes.put("description", dbName + "建库语句");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        HashMap<String, Object> instance = new HashMap<>(5);
        instance.put("guid", guidAndQualifiedNameDb.v1());
        instance.put("typeName", "clickhouse_cluster");
        attributes.put("cluster", instance);
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
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

    private static Tuple<String, String> getEntity(AtlasClientV2 atlasClientV2, String typeName, String qualifiedName) {
        try {
            AtlasSearchResult entity = atlasClientV2.basicSearch(typeName, null, null, false, 999, 0);
            return new Tuple<>(entity.getEntities().get(0).getGuid(), qualifiedName);
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
        return null;
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

    public static AtlasEntity.AtlasEntityWithExtInfo createInstance(Connection conn, String qualifiedNameInstance) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("clickhouse_cluster");
        HashMap<String, Object> attributes = new HashMap<>(10);
        Statement statement = null;
        ResultSet result = null;
        try {
            attributes.put("qualifiedName", qualifiedNameInstance);
            attributes.put("name", "clickhouse_cluster");
            attributes.put("dbms_type", "ClickHouse");
            statement = conn.createStatement();
            String versionSql = "select value as version from system.build_options where name = 'SYSTEM';";
            result = statement.executeQuery(versionSql);
            while (result.next()) {
                attributes.put("platform", result.getString(1));
            }
            attributes.put("hostname", "192.168.110.101");
            attributes.put("port", "5431");
            attributes.put("createBy", "root");
            attributes.put("owner", "测试");
            attributes.put("description", "测试");
            attributes.put("comment", "测试");
            attributes.put("protocol", "jdbc");
            attributes.put("createTime", new Date());
            atlasEntity.setAttributes(attributes);
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
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }

    private static AtlasEntity.AtlasEntityWithExtInfo createDb(Connection conn, String dbName,
                                                               String qualifiedNameDb,
                                                               Tuple<String, String> guidAndQualifiedName) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("clickhouse_db");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", qualifiedNameDb);
        attributes.put("owner", "default");
        attributes.put("ownerType", "user");
        attributes.put("name", dbName);
        ResultSet resultSet = null;
        Statement statement = null;
        try {
            String sql = "select * from system.databases where name = '" + dbName + "'";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                String engine = resultSet.getString("engine");
                String dataPath = resultSet.getString("data_path");
                String metadataPath = resultSet.getString("metadata_path");
                String comment = resultSet.getString("comment");
                attributes.put("ddl", "CREATE DATABASE " + dbName + " ENGINE = " + engine);
                attributes.put("engine", engine);
                attributes.put("data_path", dataPath);
                attributes.put("metadata_path", metadataPath);
                attributes.put("comment", comment);
                attributes.put("description", resultSet.getString("comment"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        attributes.put("createdBy", "default");
        attributes.put("createTime", new Date());
        HashMap<String, Object> instance = new HashMap<>(5);
        instance.put("guid", guidAndQualifiedName.v1());
        instance.put("typeName", "clickhouse_cluster");
        attributes.put("cluster", instance);
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }

    private static AtlasEntity.AtlasEntityWithExtInfo createTable(Connection conn, String tableName,
                                                                  String qualifiedNameTable,
                                                                  Tuple<String, String> guidAndQualifiedDbName) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("clickhouse_table");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", qualifiedNameTable);
        String dbName = qualifiedNameTable.split("\\.")[0];
        ResultSet resultSet = null;
        Statement statement = null;
        try {
            String sql = "select * from system.tables where database = '" + dbName + "' and name = '" + tableName + "'";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                attributes.put("dbName", dbName);
                attributes.put("name", tableName);
                attributes.put("owner", "root");
                attributes.put("description", resultSet.getString("comment"));
                attributes.put("type", "clickhouse_table");
                attributes.put("createdBy", "root");
                attributes.put("engine", resultSet.getString("engine"));
                attributes.put("isTemporary", resultSet.getString("is_temporary"));
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                try {
                    Date parse = simpleDateFormat.parse(resultSet.getString("metadata_modification_time"));
                    attributes.put("metadataModifiedTime", parse);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                attributes.put("ddl", resultSet.getString("create_table_query"));
                attributes.put("partitionKey", resultSet.getString("partition_key"));
                attributes.put("sortingKey", resultSet.getString("sorting_key"));
                attributes.put("primaryKey", resultSet.getString("primary_key"));
                attributes.put("storagePolicy", resultSet.getString("storage_policy"));
                attributes.put("totalRows", resultSet.getString("total_rows"));
                attributes.put("comment", resultSet.getString("comment"));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        HashMap<String, Object> db = new HashMap<>(5);
        db.put("guid", guidAndQualifiedDbName.v1());
        db.put("typeName", "clickhouse_db");
        attributes.put("db", db);
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }

    private static AtlasEntity.AtlasEntityWithExtInfo createColumn(Connection conn, String columnName,
                                                                   String qualifiedNameColumn,
                                                                   Tuple<String, String> guId) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("clickhouse_column");
        HashMap<String, Object> attributes = new HashMap<>(10);
        String[] qualifiedNameColumnSplit = qualifiedNameColumn.split("\\.");
        String dbName = qualifiedNameColumnSplit[0];
        String tableName = qualifiedNameColumnSplit[1];
        ResultSet resultSet = null;
        Statement statement = null;
        try {
            String sql = "select * from system.columns where database = '" + dbName + "' and table = '"
                    + tableName + "' and name = '" + columnName.split(":")[0] + "'";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                attributes.put("qualifiedName", qualifiedNameColumn);
                attributes.put("createdBy", "root");
                attributes.put("name", columnName.split(":")[0]);
                attributes.put("owner", "root");
                attributes.put("description", resultSet.getString("comment"));
                attributes.put("data_type", columnName.split(":")[1]);
                attributes.put("length", resultSet.getLong("numeric_precision"));
                attributes.put("comment", resultSet.getString("comment"));
                attributes.put("default_value", resultSet.getString("default_expression"));
                attributes.put("isPrimaryKey", "1".equals(resultSet.getString("is_in_primary_key")));
                attributes.put("isPartitionKey", "1".equals(resultSet.getString("is_in_partition_key")));
                attributes.put("isSortingKey", "1".equals(resultSet.getString("is_in_sorting_key")));
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
        HashMap<String, Object> table = new HashMap<>(5);
        table.put("guid", guId.v1());
        table.put("typeName", "clickhouse_table");
        attributes.put("table", table);
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }

    public static void deleteEntities(AtlasClientV2 atlasClientV2) {
        try {
            // 删除实体
            ArrayList<String> list = new ArrayList<>();
            list.add("e362f01f-619a-4d3f-b36a-66082bc003de");
            list.add("de65a90b-7c53-4d62-9cbf-962f9a36b178");
            list.add("e41c5d09-2535-4ca4-8eb8-5fd979bd38af");
            list.add("2fc1cc4a-6619-458b-b888-821f7d5d3332");
            list.add("2ea747da-3842-4e42-b67f-b7d3c8a190a5");
            list.add("4307b5ca-807c-4322-af72-b41b2787ebf2");
            atlasClientV2.deleteEntitiesByGuids(list);
            HashSet<String> objects = new HashSet<>(list);
            atlasClientV2.purgeEntitiesByGuids(objects);
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
    }


}
