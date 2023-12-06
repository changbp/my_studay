package com.cbp.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.springframework.util.ObjectUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


/**
 * Hello world!
 */
public class DeleteAtlasEntityApp {
    public static void main(String[] args) throws Exception {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.159:21000/"}, new String[]{"admin", "!QAZ2wsx3edc"});
//        File file = new File("D:\\upline\\0010-base_model.json");
//        String jsonStr = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
//        AtlasTypesDef atlasTypesDef = JSONObject.parseObject(jsonStr, AtlasTypesDef.class);
//        List<AtlasEntityDef> entityDefs = atlasTypesDef.getEntityDefs();
//        System.out.println(JSON.toJSONString(entityDefs));
//        atlasClientV2.deleteAtlasTypeDefs(atlasTypesDef);
////        atlasClientV2.createAtlasTypeDefs(atlasTypesDef);
//        atlasClientV2.updateAtlasTypeDefs(atlasTypesDef);
//        String[] typeNames = {"postgresql_instance", "postgresql_db", "postgresql_table", "postgresql_column",
//                "oracle_instance", "oracle_db", "oracle_table", "oracle_column"
//                , "sqlserver_instance", "sqlserver_db", "sqlserver_table", "sqlserver_column"
//                , "clickhouse_cluster", "clickhouse_db", "clickhouse_table", "clickhouse_column"
//        };
//        String[] typeNames = {"mysql_instance", "mysql_db", "mysql_table", "mysql_column"};
        String[] typeNames = {"DataConfidentialityLevel", "ConfigurationCatalogDef"};
//        String[] typeNames = {"ConfigurationCatalogDef"};
        for (String typeName : typeNames) {
            AtlasSearchResult result = atlasClientV2.basicSearch(typeName, null, null, false, 1000, 0);
            //清除实体信息
            if (!ObjectUtils.isEmpty(result.getEntities())) {
                result.getEntities().forEach(a -> {
                    try {
                        atlasClientV2.deleteEntityByGuid(a.getGuid());
                        HashSet<String> objects = new HashSet<>();
                        objects.add(a.getGuid() + "");
                        atlasClientV2.purgeEntitiesByGuids(objects);
                    } catch (AtlasServiceException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
//        AtlasRelationshipDef entity_instance_catalog = atlasClientV2.getRelationshipDefByName("postgresql_instance_databases");
    }
}
