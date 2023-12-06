package com.cbp.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.unicom.udme.entity.atlas.search.AtlasEntityWithExtInfo;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.profile.AtlasUserSavedSearch;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.collect.Tuple;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/11/14 10:52
 */
public class AtlasTest02 {
    public static void main(String[] args) {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.144:21000/"}, new String[]{"admin", "AtlasPSWD!23"});
        try {
            AtlasEntity.AtlasEntityWithExtInfo entityByGuid = atlasClientV2.getEntityByGuid("a8fbddc4-53cd-45e9-b76e-d333f8216cc0");
            final Map<String, Object> attributes = entityByGuid.getEntity().getAttributes();
            AtlasEntity.AtlasEntityWithExtInfo dataConfidentialityLevel = atlasClientV2.getEntityByGuid("1e0f78eb-9b48-4235-b9a8-203bc9cfba59");
            Map<String, Object> attributesDataConfidentialityLevel = dataConfidentialityLevel.getEntity().getAttributes();
            Map<String, Object> map = new HashMap<>(5);
            map.put("qualifiedName", attributesDataConfidentialityLevel.get("qualifiedName"));
            map.put("name", attributesDataConfidentialityLevel.get("name"));
            map.put("description", attributesDataConfidentialityLevel.get("description"));
            map.put("level", attributesDataConfidentialityLevel.get("level"));
            map.put("guid", "");
            attributes.put("data_security_level", map);
            attributes.put("updateTime", new Date());
            attributes.put("data_security_guid", "1e0f78eb-9b48-4235-b9a8-203bc9cfba59");
            atlasClientV2.updateEntity(entityByGuid);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public static List<String> columnExists(AtlasClientV2 atlasService, String guidAndQualifiedNameTable,
                                            List<String> qualifiedNameColumnNames) {
        try {
            AtlasEntity.AtlasEntityWithExtInfo atlasServiceEntityByGuid = atlasService.getEntityByGuid(guidAndQualifiedNameTable);
            ArrayList<String> qualifiedNameColumnNamesAtlas = new ArrayList<>();
            Map<String, AtlasEntity> referredEntities = atlasServiceEntityByGuid.getReferredEntities();
            for (String key : referredEntities.keySet()) {
                Map<String, Object> attributes = referredEntities.get(key).getAttributes();
                if (attributes != null && attributes.get("qualifiedName") != null) {
                    qualifiedNameColumnNamesAtlas.add(attributes.get("qualifiedName").toString());
                }
            }
            ArrayList<String> delQualifiedNameColumnNames = new ArrayList<>(qualifiedNameColumnNamesAtlas);
            for (String nameColumnNamesAtlas : qualifiedNameColumnNamesAtlas) {
                qualifiedNameColumnNames.forEach(columnName -> {
                    if (nameColumnNamesAtlas.contains(columnName)) {
                        delQualifiedNameColumnNames.remove(nameColumnNamesAtlas);
                    }
                });
            }
            return delQualifiedNameColumnNames;
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
        return null;
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
                        delQualifiedNameColumnNames.remove(nameColumnNamesAtlas);
                    }
                });
            }
            return delQualifiedNameColumnNames;
        } catch (AtlasServiceException e) {
            e.printStackTrace();
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
                filterCriteria.setCondition(SearchParameters.FilterCriteria.Condition.OR);
                final ArrayList<SearchParameters.FilterCriteria> criterionFilterList = new ArrayList<>();
                final SearchParameters.FilterCriteria criterionFilter = new SearchParameters.FilterCriteria();
                criterionFilter.setAttributeName(attribute);
                criterionFilter.setOperator(SearchParameters.Operator.EQ);
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
