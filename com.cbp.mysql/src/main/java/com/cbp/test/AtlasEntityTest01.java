package com.cbp.test;

import com.alibaba.fastjson.JSON;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.elasticsearch.common.collect.Tuple;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/11/29 10:30
 */
public class AtlasEntityTest01 {
    public static void main(String[] args) {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.159:21000/"}, new String[]{"admin", "!QAZ2wsx3edc"});
        try {
//            AtlasEntity.AtlasEntityWithExtInfo table = createTable("animal", "db_2023.animal@mysql",
//                    "5e1de6dc-3769-4673-90fc-a34049ba528c");
//            atlasClientV2.createEntity(table);
            AtlasEntity.AtlasEntityWithExtInfo column = createColumn("address", "db_2023.animal.address@mysql",
                    "293532f6-1eb3-430e-8998-781dec2a3614");
            atlasClientV2.createEntity(column);
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }

    }

    private static AtlasEntity.AtlasEntityWithExtInfo createColumn(String columnName,
                                                                   String qualifiedNameColumn,
                                                                   String guId) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("LogicEntityColumn");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", qualifiedNameColumn);
        attributes.put("createdBy", "root");
        attributes.put("name", columnName);
        attributes.put("cn_name", "来源");
        attributes.put("nameEng", columnName);
        attributes.put("alias", columnName);
        attributes.put("owner", "root");
        attributes.put("description", "name");
        attributes.put("createTime", new Date());
        attributes.put("updateTime", new Date());
        attributes.put("path", "主题一级/主题二级/主题三级/address");
        attributes.put("level", "L5");
        attributes.put("source_sys", "ERP");
        attributes.put("standard_name", "标准名称地球");
        attributes.put("col_data_type", "VARCHAR");
        attributes.put("default_value", "地球");
        attributes.put("isNullable", "false");
        attributes.put("isPrimaryKey", "false");
        attributes.put("length", "255");
        HashMap<String, Object> column = new HashMap<>(5);
        column.put("guid", guId);
        column.put("typeName", "LogicEntity");
        attributes.put("LogicEntity", column);
//        ArrayList<HashMap<String, Object>> list = new ArrayList<>();
//        HashMap<String, Object> hiveColumn = new HashMap<>(5);
//        hiveColumn.put("guid", "8d194e2d-0394-4f14-8b18-9876f057ce22");
//        hiveColumn.put("typeName", "hive_column");
//        list.add(hiveColumn);
//        attributes.put("HiveColumns", list);
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }

    private static AtlasEntity.AtlasEntityWithExtInfo createTable(String tableName,
                                                                  String qualifiedNameTable,
                                                                  String guidAndQualifiedDbName) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName("LogicEntity");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", qualifiedNameTable);
        attributes.put("name", tableName);
        attributes.put("owner", "root");
        attributes.put("description", "动物表逻辑实体");
        attributes.put("createTime", new Date());
        attributes.put("updateTime", new Date());
        attributes.put("path", "主题一级/主题二级/主题三级/animal");
        attributes.put("level", "L4");
        attributes.put("source_sys", "ERP");
        attributes.put("cn_name", "动物表");
        attributes.put("alias", "动物表");
        attributes.put("nameEng", tableName);
        attributes.put("catalog_guids", guidAndQualifiedDbName);
        HashMap<String, Object> catalog = new HashMap<>(5);
        catalog.put("guid", guidAndQualifiedDbName);
        catalog.put("typeName", "EntityCatalog");
        attributes.put("EntityCatalog", catalog);
        atlasEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(atlasEntity);
        return atlasEntityWithExtInfo;
    }


}
