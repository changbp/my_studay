package com.cbp.test;


import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.toolkit.ObjectUtils;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.*;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/10/30 14:13
 */
public class AtlasTest {
    public static void main(String[] args) {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.144:21000/"}, new String[]{"admin", "AtlasPSWD!23"});
//        ArrayList<String> strings1 = new ArrayList<>();
//        strings1.add("udme_api.api_group@132.35.231.167@mysql");
//        List<String> strings = tableExists(atlasClientV2, "d6319031-0695-421f-9bd8-9cbc552ac6fd", strings1);
//        System.out.println(strings);
//        QuickSearchParameters quickSearchParameters = new QuickSearchParameters();
//        quickSearchParameters.setExcludeDeletedEntities(true);
//        quickSearchParameters.setIncludeSubTypes(true);
//        quickSearchParameters.setOffset(0);
//        quickSearchParameters.setLimit(10);
//        quickSearchParameters.setQuery("*a*");
//        quickSearchParameters.setTypeName("");
//        SearchParameters.FilterCriteria filterCriteria = new SearchParameters.FilterCriteria();
//        filterCriteria.setCondition(SearchParameters.FilterCriteria.Condition.OR);
//        final ArrayList<SearchParameters.FilterCriteria> criterionFilterList = new ArrayList<>();
//        final SearchParameters.FilterCriteria criterionFilter = new SearchParameters.FilterCriteria();
//        criterionFilter.setAttributeName("name");
//        criterionFilter.setOperator(SearchParameters.Operator.CONTAINS);
//        criterionFilter.setAttributeValue("a");
//        criterionFilterList.add(criterionFilter);
//        filterCriteria.setCriterion(criterionFilterList);
//        quickSearchParameters.setEntityFilters(filterCriteria);
//        try {
//            AtlasQuickSearchResult atlasQuickSearchResult = atlasClientV2.quickSearch(quickSearchParameters);
//            System.out.println(JSON.toJSONString(atlasQuickSearchResult.getSearchResults()));
////            AtlasSuggestionsResult app = atlasClientV2.getSuggestions("app","app");
////            System.out.println(JSON.toJSONString(app.getSuggestions()));
//        } catch (AtlasServiceException e) {
//            e.printStackTrace();
//        }
//        String[] tableTypeNames = {"hive_table", "mysql_table", "oracle_table", "sqlserver_table", "postgresql_table", "clickhouse_table"};
//        BusinessCountInfo businessCountInfo = new BusinessCountInfo();
//        // 业务系统数量 默认为2，目前只有资产目录和湖仓资产
//        List<String> systemList = new ArrayList<>();
//        List<String> systemCount = getSystemCount(atlasClientV2, tableTypeNames, systemList);
//        List<String> collect = systemCount.stream().distinct().collect(Collectors.toList());
//        System.out.println(systemCount);
//        System.out.println(collect);
//        businessCountInfo.setBusinessSystemCount(collect.size());

//        try {
//            AtlasEntity.AtlasEntityWithExtInfo entityByGuid = atlasClientV2.getEntityByGuid("8dc1598b-42fe-4c6f-b0cc-93ea8fc98839");
//            System.out.println(JSON.toJSONString(entityByGuid.getEntity()));
//        } catch (AtlasServiceException e) {
//            e.printStackTrace();
//        }

//        try {
//            atlasClientV2.deleteEntityByGuid("cdd6ac88-fb38-4ab9-9c4d-fd221fa0fcd1");
//            HashSet<String> objects = new HashSet<>();
//            objects.add("cdd6ac88-fb38-4ab9-9c4d-fd221fa0fcd1");
//            atlasClientV2.purgeEntitiesByGuids(objects);
//        } catch (AtlasServiceException e) {
//            e.printStackTrace();
//        }

//        try {
//            List<EntityAuditEventV2> auditEvents = atlasClientV2.getAuditEvents("60da2ed1-45de-481f-8e9f-0aac2a36213a",
//                    "60da2ed1-45de-481f-8e9f-0aac2a36213a:1699346678329", EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE, (short) 10000);
//            List<EntityAuditEventV2> auditEvent2 = atlasClientV2.getAuditEvents("60da2ed1-45de-481f-8e9f-0aac2a36213a",
//                    "", EntityAuditEventV2.EntityAuditActionV2.ENTITY_DELETE, (short) 10000);
//            System.out.println(JSON.parseArray(JSON.toJSONString(auditEvents)));
//            System.out.println(JSON.parseArray(JSON.toJSONString(auditEvent2)));
//        } catch (AtlasServiceException e) {
//            e.printStackTrace();
//        }
//        Map<String, String> maps = new HashMap<>();
//        try {
//            AtlasEntity.AtlasEntityWithExtInfo entityByAttribute = atlasClientV2.getEntityByGuid("a0e3454d-16b3-4e04-95e9-1cf1d06fb282");
//            entityByAttribute.getEntity().getAttributes().put("catalogs", new ArrayList<AtlasObjectId>());
//            entityByAttribute.getEntity().getAttributes().put("path", "");
//            entityByAttribute.getEntity().getAttributes().put("catalog_guids", "");
//            atlasClientV2.updateEntity(entityByAttribute);
//        } catch (AtlasServiceException e) {
//            e.printStackTrace();
//        }
//        AtlasSearchResult searchResult = basic(atlasClientV2, "mysql_table");
//        if (searchResult != null) {
//            List<AtlasEntityHeader> entities = searchResult.getEntities();
//            List<AtlasEntityHeader> db1 = entities.stream().filter(e -> {
//                JSONObject db = JSON.parseObject(JSON.toJSONString(e.getAttributes().get("db")));
//                Object guid = db.get("guid");
//                return "14fd534d-6cad-462b-80bb-b8a86c65671b".equals(guid);
//            }).collect(Collectors.toList());
//            searchResult.setEntities(db1);
//            System.out.println(JSON.toJSONString(searchResult));
//        }

//        HashMap<String, String> stringStringHashMap = new HashMap<>();
//        stringStringHashMap.put("qualifiedName","test_changan.swd_acategory@132.35.231.167@mysql");
//        try {
//            AtlasEntity.AtlasEntityWithExtInfo mysql_table = atlasClientV2.getEntityByAttribute("mysql_table", stringStringHashMap);
//            System.out.println(mysql_table.getEntity().getGuid());
//        } catch (AtlasServiceException e) {
//            e.printStackTrace();
//        }
//        try {
//            AtlasEntity.AtlasEntityWithExtInfo entityByGuid = atlasClientV2.getEntityByGuid("fec47470-c653-4f23-a591-065d0dbb452f");
//            Object tables = entityByGuid.getEntity().getAttributes().get("tables");
//            JSONArray tableArr = JSON.parseArray(JSON.toJSONString(tables));
//            ArrayList<AtlasEntity.AtlasEntityWithExtInfo> list = new ArrayList<>();
//            tableArr.forEach(table -> {
//                Object tableGuid = JSON.parseObject(table.toString()).get("guid");
//                try {
//                    AtlasEntity.AtlasEntityWithExtInfo tableEntityByGuid = atlasClientV2.getEntityByGuid(tableGuid.toString());
//                    list.add(tableEntityByGuid);
//                } catch (AtlasServiceException e) {
//                    e.printStackTrace();
//                }
//            });
//            System.out.println(JSON.toJSONString(list));
//        } catch (AtlasServiceException e) {
//            e.printStackTrace();
//        }
//        try {
//            AtlasEntity.AtlasEntityWithExtInfo atlasSearchResult = atlasClientV2.getEntityByGuid("8aa96c85-8e5f-41a4-b742-ed0efcbe2d8c");
//            AtlasEntity entity = atlasSearchResult.getEntity();
//            AtlasEntity.Status status = entity.getStatus();
//            Map<String, Object> attributes = entity.getAttributes();
//            Object columns = attributes.get("columns");
//            String s = JSON.toJSONString(columns);
//            JSONArray jsonArray = JSON.parseArray(s);
//            jsonArray.forEach(c -> {
//                JSONObject jsonObject = JSON.parseObject(JSON.toJSONString(c));
//                try {
//                    AtlasEntity.AtlasEntityWithExtInfo columnEntity = atlasClientV2.getEntityByGuid(jsonObject.get("guid").toString());
//                    Object path = columnEntity.getEntity().getAttributes().get("path");
//                    String result = ObjectUtils.isEmpty(path) ? "11" : path.toString();
//                    System.out.println(result);
//                } catch (AtlasServiceException e) {
//                    e.printStackTrace();
//                }
////                System.out.println(jsonObject);
//            });
//            System.out.println(jsonArray);
//        } catch (AtlasServiceException e) {
//            e.printStackTrace();
//        }
//        Tuple<String, String> entity = getEntity("mysql_table", "test_changan.swd_acategory@132.35.231.167@mysql");
//        System.out.println(entity);

//        String typeName = "EntityCatalog";
//        String attribute = "level";
//        String attributeValue = "L" + 4;
//        AtlasSearchResult searchResult = getSearchResult(atlasClientV2, typeName, attribute, attributeValue);
//        if (searchResult != null) {
//            List<AtlasEntityHeader> entities = searchResult.getEntities();
//            if (entities != null && !entities.isEmpty()) {
//                System.out.println("数据资产树存在" + attributeValue + "级目录或实体，当前目录层级不可删除！");
//            } else {
//                // 删除目录层级实体
//                try {
//                    atlasClientV2.deleteEntityByGuid("");
//                } catch (AtlasServiceException e) {
//                    e.printStackTrace();
//                }
//                System.out.println("删除目录层级成功");
//            }
//        }
    }

    public static List<String> tableExists(AtlasClientV2 atlasService, String guidAndQualifiedNameDb,
                                           ArrayList<String> qualifiedNameTables) {
        try {
            AtlasEntity.AtlasEntityWithExtInfo entityByGuid = atlasService.getEntityByGuid(guidAndQualifiedNameDb);
            ArrayList<String> qualifiedNameTablesAtlas = new ArrayList<>();
            Map<String, AtlasEntity> referredEntities = entityByGuid.getReferredEntities();
            for (String key : referredEntities.keySet()) {
                Map<String, Object> attributes = referredEntities.get(key).getAttributes();
                if (attributes != null && attributes.get("qualifiedName") != null) {
                    qualifiedNameTablesAtlas.add(attributes.get("qualifiedName").toString());
                }
            }
            ArrayList<String> delQualifiedNameColumnNames = new ArrayList<>(qualifiedNameTablesAtlas);
            for (String nameColumnNamesAtlas : qualifiedNameTablesAtlas) {
                qualifiedNameTables.forEach(e -> {
                    if (nameColumnNamesAtlas.contains(e)) {
                        qualifiedNameTablesAtlas.remove(nameColumnNamesAtlas);
                    }
                });
            }
            return delQualifiedNameColumnNames;
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
        return null;
    }


    private static List<String> getSystemCount(AtlasClientV2 atlasClientV2, String[] typeNames, List<String> systemList) {
        try {
            String typeName = String.join(",", typeNames);
            org.apache.atlas.model.discovery.SearchParameters searchParameters = new org.apache.atlas.model.discovery.SearchParameters();
            searchParameters.setExcludeDeletedEntities(true);
            searchParameters.setIncludeClassificationAttributes(true);
            searchParameters.setIncludeSubClassifications(true);
            searchParameters.setIncludeSubTypes(true);
            searchParameters.setTypeName(typeName);
            searchParameters.setOffset(0);
            searchParameters.setLimit(1000);
            Set<String> attributes = new HashSet<>();
            attributes.add("datasource_name");
            searchParameters.setAttributes(attributes);
            AtlasSearchResult result = atlasClientV2.basicSearch(searchParameters);
            List<AtlasEntityHeader> entities = result.getEntities();
            if (entities != null) {
                entities.forEach(entity -> {
                    Object datasourceName = entity.getAttributes().get("datasource_name");
                    if (!ObjectUtils.isEmpty(datasourceName)) {
                        systemList.add(datasourceName.toString());
                    }
                });
            }
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
        return systemList;
    }

    private static AtlasSearchResult getCommandResult(AtlasClientV2 atlasClientV2, String typeName) {
        SearchParameters searchParameters = new SearchParameters();
        searchParameters.setExcludeDeletedEntities(true);
        searchParameters.setIncludeClassificationAttributes(true);
        searchParameters.setIncludeSubClassifications(true);
        searchParameters.setIncludeSubTypes(true);
        searchParameters.setTypeName(typeName);
        searchParameters.setOffset(0);
        searchParameters.setLimit(1000);
        try {
            return atlasClientV2.basicSearch(searchParameters);
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean getCatalogNameExists(AtlasClientV2 atlasClientV2, String catalogName) {
        AtlasSearchResult name = getSearchResult(atlasClientV2, "EntityCatalog", "name", catalogName);
        if (name != null) {
            List<AtlasEntityHeader> entities = name.getEntities();
            if (entities != null && entities.size() > 0) {
                return false;
            }
        }
        return true;
    }

    private static AtlasEntity.AtlasEntitiesWithExtInfo getEntity(AtlasClientV2 atlasClientV2, String attribute, String attributeValue) {
        List<Map<String, String>> list = new ArrayList<>();
        Map<String, String> attributeMap = new HashMap<>(6);
        attributeMap.put(attribute, attributeValue);
        list.add(attributeMap);
        try {
            return atlasClientV2.getEntitiesByAttribute("EntityCatalog", list);
        } catch (AtlasServiceException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public static AtlasSearchResult basic(AtlasClientV2 atlasClientV2, String typeName) {
        try {
            SearchParameters searchParameters = new SearchParameters();
            searchParameters.setExcludeDeletedEntities(true);
            searchParameters.setIncludeClassificationAttributes(true);
            searchParameters.setIncludeSubClassifications(true);
            searchParameters.setIncludeSubTypes(true);
            searchParameters.setTypeName(typeName);
            searchParameters.setOffset(0);
            searchParameters.setLimit(100);
            HashSet<String> attributes = new HashSet<>(6);
            attributes.add("db");
            searchParameters.setAttributes(attributes);
            return atlasClientV2.basicSearch(searchParameters);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    private static AtlasSearchResult getSearchResult(AtlasClientV2 atlasClientV2, String typeName, String attribute, String attributeValue) {
        try {
            SearchParameters searchParameters = new SearchParameters();
            searchParameters.setExcludeDeletedEntities(true);
            searchParameters.setIncludeClassificationAttributes(true);
            searchParameters.setIncludeSubClassifications(true);
            searchParameters.setIncludeSubTypes(true);
            searchParameters.setTypeName(typeName);
            searchParameters.setOffset(0);
            searchParameters.setLimit(1000);
            if (StringUtils.isNotEmpty(attribute)) {
                HashSet<String> attributes = new HashSet<>(6);
                attributes.add(attribute);
                searchParameters.setAttributes(attributes);
                SearchParameters.FilterCriteria filterCriteria = new SearchParameters.FilterCriteria();
                filterCriteria.setCondition(SearchParameters.FilterCriteria.Condition.AND);
                final ArrayList<SearchParameters.FilterCriteria> criterionFilterList = new ArrayList<>();
                final SearchParameters.FilterCriteria criterionFilter = new SearchParameters.FilterCriteria();
                criterionFilter.setAttributeName(attribute);
                criterionFilter.setOperator(SearchParameters.Operator.CONTAINS);
                criterionFilter.setAttributeValue(attributeValue);
                criterionFilterList.add(criterionFilter);
                filterCriteria.setCriterion(criterionFilterList);
                searchParameters.setEntityFilters(filterCriteria);
            }
            return atlasClientV2.basicSearch(searchParameters);
        } catch (AtlasServiceException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }
}
