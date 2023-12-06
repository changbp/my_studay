package com.cbp.test;

import com.alibaba.fastjson.JSON;
import com.unicom.udme.entity.atlas.req.AuditReq;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.ObjectUtils;

import java.util.*;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/12/1 12:31
 */
public class Tetst000000001 {
    public static void main(String[] args) {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.159:21000/"}, new String[]{"admin", "!QAZ2wsx3edc"});
        AtlasSearchResult entityCatalogsByQuery = getEntityCatalogsByQuery(atlasClientV2);
        System.out.println(entityCatalogsByQuery);
    }

    public static AtlasSearchResult getEntityCatalogsByQuery(AtlasClientV2 atlasClientV2) {
        try {
            String guid = "7df5748e-0ac9-4319-ad0b-5cb12f8364eb";
            Set<String> showColumns = new HashSet<>();
            Integer limit = 25;
            Integer offset = 0;
            SearchParameters searchParameters = new SearchParameters();
            searchParameters.setExcludeDeletedEntities(true);
            searchParameters.setIncludeClassificationAttributes(true);
            searchParameters.setIncludeSubClassifications(true);
            searchParameters.setIncludeSubTypes(true);
            searchParameters.setTypeName("EntityCatalog");
            searchParameters.setOffset(offset);
            searchParameters.setLimit(limit);
            searchParameters.setAttributes(showColumns);

            final ArrayList<SearchParameters.FilterCriteria> criterionFilterList = new ArrayList<>();
            SearchParameters.FilterCriteria filterCriteria = new SearchParameters.FilterCriteria();
            filterCriteria.setCondition(SearchParameters.FilterCriteria.Condition.AND);
            if (!ObjectUtils.isEmpty("主题")) {
                final SearchParameters.FilterCriteria criterionFilter = new SearchParameters.FilterCriteria();
                criterionFilter.setAttributeName("name");
                criterionFilter.setOperator(SearchParameters.Operator.CONTAINS);
                criterionFilter.setAttributeValue("主题");
                criterionFilterList.add(criterionFilter);
            }
            if (!ObjectUtils.isEmpty("")) {
                final SearchParameters.FilterCriteria criterionFilter = new SearchParameters.FilterCriteria();
                criterionFilter.setAttributeName("code");
                criterionFilter.setOperator(SearchParameters.Operator.CONTAINS);
                criterionFilter.setAttributeValue("");
                criterionFilterList.add(criterionFilter);
            }
            if (!ObjectUtils.isEmpty("")) {
                final SearchParameters.FilterCriteria criterionFilter = new SearchParameters.FilterCriteria();
                criterionFilter.setAttributeName("owner");
                criterionFilter.setOperator(SearchParameters.Operator.CONTAINS);
                criterionFilter.setAttributeValue("");
                criterionFilterList.add(criterionFilter);
            }
            if (!ObjectUtils.isEmpty("")) {
                final SearchParameters.FilterCriteria criterionFilter = new SearchParameters.FilterCriteria();
                criterionFilter.setAttributeName("dataOwner");
                criterionFilter.setOperator(SearchParameters.Operator.CONTAINS);
                criterionFilter.setAttributeValue("");
                criterionFilterList.add(criterionFilter);
            }
            if (!ObjectUtils.isEmpty("ddd")) {
                final SearchParameters.FilterCriteria criterionFilter = new SearchParameters.FilterCriteria();
                criterionFilter.setAttributeName("updateTime");
                criterionFilter.setOperator(SearchParameters.Operator.GTE);
                criterionFilter.setAttributeValue("1698826510000");
                criterionFilterList.add(criterionFilter);
            }
            if (!ObjectUtils.isEmpty("ddd")) {
                final SearchParameters.FilterCriteria criterionFilter = new SearchParameters.FilterCriteria();
                criterionFilter.setAttributeName("updateTime");
                criterionFilter.setOperator(SearchParameters.Operator.LTE);
                criterionFilter.setAttributeValue("1701504959000");
                criterionFilterList.add(criterionFilter);
            }
            final SearchParameters.FilterCriteria criterionFilter = new SearchParameters.FilterCriteria();
            criterionFilter.setAttributeName("parentGuid");
            criterionFilter.setOperator(SearchParameters.Operator.EQ);
            criterionFilter.setAttributeValue(guid);
            criterionFilterList.add(criterionFilter);
            filterCriteria.setCriterion(criterionFilterList);
            searchParameters.setEntityFilters(filterCriteria);
            System.out.println(JSON.toJSONString(searchParameters));
            return atlasClientV2.basicSearch(searchParameters);
        } catch (AtlasServiceException e) {
            System.out.println(e.getMessage());
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
            e.getMessage();
        }
        return null;
    }

}
