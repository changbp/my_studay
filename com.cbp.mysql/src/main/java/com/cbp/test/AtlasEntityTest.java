package com.cbp.test;

import com.unicom.udme.common.util.StringUtils;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.type.AtlasTypeUtil;
import org.springframework.util.ObjectUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/10/25 16:06
 */
public class AtlasEntityTest {

    public static void main(String[] args) {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.144:21000/"}, new String[]{"admin", "AtlasPSWD!23"});
//        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.159:21000/"}, new String[]{"admin", "!QAZ2wsx3edc"});
        try {
            EntityMutationResponse entity = createEntity(atlasClientV2);
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
//        AtlasEntity.AtlasEntityWithExtInfo lineage = createLineage();
//        try {
//            atlasClientV2.createEntity(lineage);
//        } catch (AtlasServiceException e) {
//            e.printStackTrace();
//        }
        // 创建血缘

        // 查询
//        extracted(atlasClientV2);
    }

    public static AtlasEntity.AtlasEntityWithExtInfo createLineage() {
        AtlasEntity lineageEntity = new AtlasEntity();
        lineageEntity.setTypeName("Process");
        HashMap<String, Object> attributes = new HashMap<>(10);
        attributes.put("qualifiedName", "api_app_api_group");
        attributes.put("name", "apiProcess");
        attributes.put("description", "测试创建血缘");
        attributes.put("type", "table");
        List<Map<String, String>> inputList = new ArrayList<>();
        Map<String, String> inputMap = new HashMap<>();
        inputMap.put("guid", "8f9b1961-10f2-444d-b31c-6eaf4f39c774");
        inputMap.put("typeName", "mysql_table");
        inputList.add(inputMap);
        List<Map<String, String>> outputList = new ArrayList<>();
        Map<String, String> outputMap = new HashMap<>();
        outputMap.put("guid", "f5dae0cf-5bbc-4e1f-b456-348cbce8bdda");
        outputMap.put("typeName", "mysql_table");
        outputList.add(outputMap);
        attributes.put("inputs", inputList);
        attributes.put("outputs", outputList);
        lineageEntity.setAttributes(attributes);
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        atlasEntityWithExtInfo.setEntity(lineageEntity);
        return atlasEntityWithExtInfo;
    }

    private static void extracted(AtlasClientV2 atlasClientV2) {
        // 查询
        String[] dbTypeNames = {"hive_table", "mysql_table", "oracle_table", "sqlserver_table", "postgresql_table", "clickhouse_table"};
        HashMap<String, Long> businessDatabases = new HashMap<>(6);
        for (String typeName : dbTypeNames) {
            try {
                AtlasSearchResult basic = atlasClientV2.basicSearch(typeName, null, null,
                        true, 900, 0);
                System.out.println(basic);
                long approximateCount = basic.getApproximateCount();
                int size = basic.getEntities() == null ? 0 : basic.getEntities().size();
                if (size <= 1000) {
                    businessDatabases.put(typeName, (long) size);
                } else {
                    businessDatabases.put(typeName, approximateCount);
                }
            } catch (AtlasServiceException e) {
                e.printStackTrace();
            }
        }
        Long businessDatabaseCount = businessDatabases.values().stream().reduce(0L, Long::sum);
        System.out.println(businessDatabaseCount);
    }


    public static EntityMutationResponse createEntity(AtlasClientV2 atlasClientV2) throws AtlasServiceException {
        try {
            final AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
            AtlasEntity atlasEntity = new AtlasEntity();
            atlasEntity.setTypeName("BusinessLogicEntity");
            atlasEntity.setCreateTime(new Date());
            atlasEntity.setUpdateTime(new Date());
            atlasEntity.setGuid("20230830001");
            Map<String, Object> attributes = new HashMap<>(10);
            attributes.put("owner", "lyt");
            attributes.put("level", "4");
            attributes.put("qualifiedName", "L4_8c9d4df9-51de-42ba-a89c-9efe70d200f1");
            attributes.put("nameEng", "测试实体");
            attributes.put("path", "水务安全/水务网络信息/水务开发/L4-逻辑实体");
            attributes.put("name", "L4-逻辑实体");
            attributes.put("description", "定义0bc10c9e-b71a-4650-ba8f-d3e48604bd9c");
            attributes.put("createTime", new Date());
            attributes.put("updateTime", new Date());
            attributes.put("code", "30550335-e476-4ff4-810f-5fee4dcc0a1f");
            attributes.put("source_sys", "湖仓资产");
            attributes.put("desc", "定义0bc10c9e-b71a-4650-ba8f-d3e48604bd9c");
            atlasEntity.setStatus(AtlasEntity.Status.ACTIVE);
            atlasEntity.setAttributes(attributes);
            AtlasObjectId atlasObjectId = new AtlasObjectId();
            atlasObjectId.setGuid("1076162184620408832");
            atlasObjectId.setTypeName("BusinessCatalog");
            atlasEntity.setRelationshipAttribute("parent", atlasObjectId);
            ArrayList<AtlasObjectId> atlasObjectIds = new ArrayList<>();
            AtlasObjectId atlasObjectId1 = new AtlasObjectId();
            atlasObjectId1.setGuid("20230830003");
            atlasObjectId1.setTypeName("BusinessLogicEntityColumn");
            atlasObjectIds.add(atlasObjectId1);
            AtlasObjectId atlasObjectId2 = new AtlasObjectId();
            atlasObjectId2.setGuid("20230830002");
            atlasObjectId2.setTypeName("BusinessLogicEntityColumn");
            atlasObjectIds.add(atlasObjectId2);
            atlasEntity.setRelationshipAttribute("fields", atlasObjectIds);
            atlasEntityWithExtInfo.setEntity(atlasEntity);
            EntityMutationResponse entity = atlasClientV2.createEntity(atlasEntityWithExtInfo);
            return entity;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    private static AtlasSearchResult getCommandResult(AtlasClientV2 atlasClientV2, String attribute) throws AtlasServiceException {
        SearchParameters.FilterCriteria filterCriteria = new SearchParameters.FilterCriteria();
        filterCriteria.setCondition(SearchParameters.FilterCriteria.Condition.AND);
        ArrayList<SearchParameters.FilterCriteria> criterionFilterList = new ArrayList<>();
        SearchParameters.FilterCriteria criterionFilter = new SearchParameters.FilterCriteria();
        criterionFilter.setAttributeName(attribute);
        criterionFilter.setOperator(SearchParameters.Operator.EQ);
        criterionFilter.setAttributeValue("L3");
        criterionFilterList.add(criterionFilter);
        filterCriteria.setCriterion(criterionFilterList);
        return atlasClientV2.basicSearch("EntityCatalog", filterCriteria, null, null,
                false, 900, 0);
    }
}
