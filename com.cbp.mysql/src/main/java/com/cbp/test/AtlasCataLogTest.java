package com.cbp.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.clickhouse.client.internal.google.protobuf.compiler.PluginProtos;
import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.unicom.udme.common.util.StringUtils;
import com.unicom.udme.constants.Constants;
import com.unicom.udme.entity.atlas.req.AddEntityCatalogReq;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasTypeUtil;
import org.springframework.util.ObjectUtils;
import shaded.parquet.org.codehaus.jackson.JsonParser;

import javax.ws.rs.core.MultivaluedMap;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/10/27 11:04
 */
public class AtlasCataLogTest {

    public static void main(String[] args) {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.144:21000/"}, new String[]{"admin", "AtlasPSWD!23"});
//        MultivaluedMap<String, String> searchParams = new MultivaluedMapImpl();
//        searchParams.add(SearchFilter.PARAM_TYPE, "CLASSIFICATION");
//        SearchFilter searchFilter = new SearchFilter(searchParams);
//        AtlasTypesDef allTypeDefs = null;
//        try {
//            allTypeDefs = atlasClientV2.getAllTypeDefs(searchFilter);
//            List<AtlasClassificationDef> classificationDefs = allTypeDefs.getClassificationDefs()
//                    .stream()
//                    .sorted(Comparator.comparing(AtlasClassificationDef::getCreateTime).reversed())
//                    .collect(Collectors.toList());
//            System.out.println(classificationDefs);
//        } catch (AtlasServiceException e) {
//            e.printStackTrace();
//        }
        ArrayList<AddEntityCatalogReq> addEntityCatalogReqs = new ArrayList<>();
        AddEntityCatalogReq addEntityCatalogReq = new AddEntityCatalogReq();
        addEntityCatalogReq.setAlias("本地测试三级V2");
        addEntityCatalogReq.setCode("46544");
        addEntityCatalogReq.setCreatedBy("changbp");
        addEntityCatalogReq.setDataDepartment("67");
        addEntityCatalogReq.setDataOwner("47");
        addEntityCatalogReq.setDesc("本地测试三级V2");
        addEntityCatalogReq.setGuid("a2b75522-7394-4287-baca-6838bcf3e9f0");
        addEntityCatalogReq.setLevel("L3");
        addEntityCatalogReq.setName("本地测试三级V2");
        addEntityCatalogReq.setPath("本地测试/本地测试二级/46544");
        addEntityCatalogReq.setQualifiedName("本地测试.本地测试二级.46544@EntityCatalog");
        addEntityCatalogReq.setOrder("1");
        addEntityCatalogReq.setParentGuid("7b7709f8-03d9-4634-a11f-5459afa42a15");
        ArrayList<Map<String, String>> objects = new ArrayList<>();
        HashMap<String, String> objectObjectHashMap = new HashMap<>();
        objectObjectHashMap.put("guid","4f2be504-564b-4f6c-bce2-50ac41769761");
        objectObjectHashMap.put("typeName","postgresql_table");
        objects.add(objectObjectHashMap);
        HashMap<String, String> objectObjectHashMap1 = new HashMap<>();
        objectObjectHashMap1.put("guid","4f2be504-564b-4f6c-bce2-50ac41769761");
        objectObjectHashMap1.put("typeName","postgresql_table");
        objects.add(objectObjectHashMap1);
        addEntityCatalogReq.setTables(objects);
        addEntityCatalogReqs.add(addEntityCatalogReq);
        AddEntityCatalogReq addEntityCatalogReq2 = new AddEntityCatalogReq();
        addEntityCatalogReq2.setAlias("本地测试三级V1");
        addEntityCatalogReq2.setCode("46544");
        addEntityCatalogReq2.setCreatedBy("changbp");
        addEntityCatalogReq2.setDataDepartment("67");
        addEntityCatalogReq2.setDataOwner("47");
        addEntityCatalogReq2.setGuid("c07ab919-5aa3-4818-a612-d45ec2ddb779");
        addEntityCatalogReq2.setLevel("L3");
        addEntityCatalogReq2.setDesc("本地测试三级V1");
        addEntityCatalogReq2.setName("本地测试三级V1");
        addEntityCatalogReq2.setPath("本地测试/本地测试二级/3434");
        addEntityCatalogReq2.setQualifiedName("本地测试.本地测试二级.3434@EntityCatalog");
        addEntityCatalogReq2.setOrder("2");
        addEntityCatalogReq2.setParentGuid("7b7709f8-03d9-4634-a11f-5459afa42a15");
        addEntityCatalogReq.setTables(new ArrayList<>());
        addEntityCatalogReqs.add(addEntityCatalogReq2);

        addEntityCatalogReqs.forEach(line -> {
            addEntityCatalog(atlasClientV2, line);
        });
    }


    public static Object addEntityCatalog(AtlasClientV2 atlasClientV2, AddEntityCatalogReq addEntityCatalogReq) {
        try {
            final AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
            AtlasEntity atlasEntity = new AtlasEntity();
            atlasEntity.setCreatedBy(addEntityCatalogReq.getCreatedBy());
            atlasEntity.setTypeName("EntityCatalog");
            atlasEntity.setCreateTime(new Date());
            atlasEntity.setUpdateTime(new Date());
            atlasEntity.setUpdatedBy(addEntityCatalogReq.getUpdatedBy());
            atlasEntity.setGuid(addEntityCatalogReq.getGuid());
            Map<String, Object> attributes = new HashMap<>(10);
            attributes.put("owner", addEntityCatalogReq.getCreatedBy());
            attributes.put("level", addEntityCatalogReq.getLevel());
            attributes.put("qualifiedName", addEntityCatalogReq.getQualifiedName());
            attributes.put("nameEng", addEntityCatalogReq.getNameEng());
            attributes.put("path", addEntityCatalogReq.getPath());
            attributes.put("name", addEntityCatalogReq.getName());
            attributes.put("description", addEntityCatalogReq.getDesc());
            attributes.put("createTime", new Date());
            attributes.put("updateTime", new Date());
            attributes.put("dataOwner", addEntityCatalogReq.getDataOwner());
            attributes.put("dataDepartment", addEntityCatalogReq.getDataDepartment());
            attributes.put("code", addEntityCatalogReq.getCode());
            attributes.put("alias", addEntityCatalogReq.getAlias());
            attributes.put("source_sys", "test");
            attributes.put("order", addEntityCatalogReq.getOrder());
            atlasEntity.setStatus(AtlasEntity.Status.ACTIVE);
            atlasEntity.setAttributes(attributes);
            atlasEntityWithExtInfo.setEntity(atlasEntity);
            // EntityMutationResponse entity = atlasClientV2.createEntity(atlasEntityWithExtInfo);
            //设置父目录关系
            if (StringUtils.isEmpty(addEntityCatalogReq.getParentGuid())) {
                atlasEntity.setRelationshipAttribute("parent", null);
            } else {
                AtlasEntity.AtlasEntityWithExtInfo entityParent = atlasClientV2.getEntityByGuid(addEntityCatalogReq.getParentGuid());
                atlasEntity.setRelationshipAttribute("parent", AtlasTypeUtil.getAtlasObjectId(entityParent.getEntity()));
            }
            //设置子目录关系
            if (!ObjectUtils.isEmpty(addEntityCatalogReq.getChilds())) {

                List<AtlasObjectId> atlasObjectIds = addEntityCatalogReq.getChilds().stream().map(a -> {
                    AtlasObjectId atlasObjectId = new AtlasObjectId();
                    atlasObjectId.setGuid(a);
                    atlasObjectId.setTypeName("EntityCatalog");
                    return atlasObjectId;
                }).distinct().collect(Collectors.toList());

                atlasEntity.setRelationshipAttribute("childs", atlasObjectIds);
            }
            //设置tables关系
            if (!ObjectUtils.isEmpty(addEntityCatalogReq.getTables())) {
                List<AtlasObjectId> atlasObjectIds = addEntityCatalogReq.getTables().stream().map(a -> {
                    AtlasObjectId atlasObjectId = new AtlasObjectId();
                    atlasObjectId.setGuid(a.get("guid"));
                    atlasObjectId.setTypeName(a.get("typeName"));
                    return atlasObjectId;
                }).distinct().collect(Collectors.toList());
                atlasEntity.setRelationshipAttribute("tables", atlasObjectIds);
            }
            EntityMutationResponse entity = atlasClientV2.createEntity(atlasEntityWithExtInfo);
            return entity;
        } catch (Exception ex) {
            System.out.println("添加实体目录出错" + ex);
            ex.printStackTrace();
        }
        return null;
    }


}
