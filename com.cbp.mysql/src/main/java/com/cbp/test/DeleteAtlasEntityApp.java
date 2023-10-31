package com.cbp.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.springframework.util.ObjectUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;


/**
 * Hello world!
 */
public class DeleteAtlasEntityApp {
    public static void main(String[] args) throws Exception {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.144:21000/"}, new String[]{"admin", "AtlasPSWD!23"});
        File file = new File("D:\\rdbms\\upLine\\10000-mysql_model.json");
        String jsonStr = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        AtlasTypesDef atlasTypesDef = JSONObject.parseObject(jsonStr, AtlasTypesDef.class);
        AtlasSearchResult businessCatalog1 = atlasClientV2.basicSearch("EntityCatalog", null,
                null, false, 999, 0);
        //清除实体信息
        if (!ObjectUtils.isEmpty(businessCatalog1.getEntities())) {
            businessCatalog1.getEntities().forEach(a -> {
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
//        AtlasRelationshipDef entity_instance_catalog = atlasClientV2.getRelationshipDefByName("postgresql_instance_databases");
//        atlasClientV2.deleteAtlasTypeDefs(atlasTypesDef);
//        atlasClientV2.createAtlasTypeDefs(atlasTypesDef);
    }


}
