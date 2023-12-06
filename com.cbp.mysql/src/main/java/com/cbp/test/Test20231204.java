package com.cbp.test;

import com.alibaba.fastjson.JSON;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/12/4 11:06
 */
public class Test20231204 {
    public static void main(String[] args) {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.144:21000/"},
                new String[]{"admin", "AtlasPSWD!23"});
        try {
            SearchParameters searchParameters = new SearchParameters();
            searchParameters.setExcludeDeletedEntities(true);
            searchParameters.setIncludeClassificationAttributes(true);
            searchParameters.setIncludeSubClassifications(true);
            searchParameters.setIncludeSubTypes(true);
            HashSet<String> typeNames = new HashSet<>();
            typeNames.add("mysql_table");
            typeNames.add("oracle_table");
            typeNames.add("postgresql_table");
            typeNames.add("sqlserver_table");
            typeNames.add("clickhouse_table");
            String typeNameDef = String.join(",", typeNames);
            searchParameters.setTypeName(typeNameDef);
            searchParameters.setOffset(0);
            searchParameters.setLimit(25);
            SearchParameters.FilterCriteria filterCriteria = new SearchParameters.FilterCriteria();
            filterCriteria.setCondition(SearchParameters.FilterCriteria.Condition.OR);
            final ArrayList<SearchParameters.FilterCriteria> criterionFilterList = new ArrayList<>();
            List<String> nameReqs = new ArrayList<>();
            nameReqs.add("业务资产数据");
            nameReqs.add("主数据");
            nameReqs.forEach(name -> {
                final SearchParameters.FilterCriteria criterionFilter = new SearchParameters.FilterCriteria();
                criterionFilter.setAttributeName("__classificationNames");
                criterionFilter.setOperator(SearchParameters.Operator.EQ);
                criterionFilter.setAttributeValue(name);
                criterionFilterList.add(criterionFilter);
            });
            filterCriteria.setCriterion(criterionFilterList);
            searchParameters.setEntityFilters(filterCriteria);
            AtlasSearchResult result = atlasClientV2.basicSearch(searchParameters);
            System.out.println(JSON.toJSONString(result));
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }


    }
}
