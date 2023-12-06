package com.cbp.test;

import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/12/5 14:34
 */
public class AtlasEntityTest2023125 {
    public static void main(String[] args) {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.159:21000/"}, new String[]{"admin", "!QAZ2wsx3edc"});
//        createDb(atlasClientV2);
        createTable(atlasClientV2);

    }



    private static void createTable(AtlasClientV2 atlasClientV2) {
        try {
            AtlasEntity tableEntity = new AtlasEntity();
            tableEntity.setTypeName("hive_table");
            HashMap<String, Object> attributes = new HashMap<>(10);
            attributes.put("qualifiedName", "hive_db_2023.hive_table02@192.168.110.101@hive_db");
            attributes.put("name", "hive_table02");
            attributes.put("owner", "deployer");
            attributes.put("description", "hive测试表02");
            attributes.put("createTime", new Date());
            attributes.put("lastAccessTime", new Date());
            attributes.put("tableType", "MANAGED_TABLE");
            attributes.put("temporary", "false");
            attributes.put("comment", "hive测试表02");
            attributes.put("clusterName", "hadoop17");
            attributes.put("dbName", "hive_db_2023");
            attributes.put("ddl", "create table hive_table02(id int,name string)");
            attributes.put("totalSize", 6);
            attributes.put("retention", 0);
            HashMap<String, Object> db = new HashMap<>(5);
            db.put("guid", "3157a555-f1c5-4ad0-b193-d9655302125b");
            db.put("typeName", "hive_db");
            attributes.put("db", db);
            HashMap<String, Object> logicEntity = new HashMap<>(5);
            logicEntity.put("guid", "929ba932-90a8-4c13-a5af-0e4c9a74942b");
            logicEntity.put("typeName", "LogicEntity");
            attributes.put("LogicEntity", logicEntity);
            tableEntity.setAttributes(attributes);
            AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
            atlasEntityWithExtInfo.setEntity(tableEntity);
            atlasClientV2.createEntity(atlasEntityWithExtInfo);
        } catch (AtlasServiceException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void createDb(AtlasClientV2 atlasClientV2) {
        // create db
        try {
            AtlasEntity dbEntity = new AtlasEntity();
            dbEntity.setTypeName("hive_db");
            HashMap<String, Object> dbAttributes = new HashMap<>(10);
            dbAttributes.put("qualifiedName", "hive_db_2023@192.168.110.101@hive_db");
            dbAttributes.put("owner", "root");
            dbAttributes.put("ownerType", "user");
            dbAttributes.put("name", "hive_db_2023");
            dbAttributes.put("clusterName", "primary");
            dbAttributes.put("description", "hive_db_2023数据库");
            dbAttributes.put("location", "hdfs://hadoop17/user/hive/warehouse/hive_db_2023.db");
            dbEntity.setAttributes(dbAttributes);
            AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
            atlasEntityWithExtInfo.setEntity(dbEntity);
            atlasClientV2.createEntity(atlasEntityWithExtInfo);
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
    }
}
