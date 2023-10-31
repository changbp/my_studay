package com.cbp.test;

import com.unicom.udme.common.util.StringUtils;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasEntityHeader;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/10/30 14:13
 */
public class AtlasTest {
    public static void main(String[] args) {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.144:21000/"}, new String[]{"admin", "AtlasPSWD!23"});
        String typeName = "EntityCatalog";
        String attribute = "level";
        String attributeValue = "L" + 4;
        AtlasSearchResult searchResult = getSearchResult(atlasClientV2, typeName, attribute, attributeValue);
        if (searchResult != null) {
            List<AtlasEntityHeader> entities = searchResult.getEntities();
            if (entities != null && !entities.isEmpty()) {
                System.out.println("数据资产树存在" + attributeValue + "级目录或实体，当前目录层级不可删除！");
            } else {
                // 删除目录层级实体
                try {
                    atlasClientV2.deleteEntityByGuid("");
                } catch (AtlasServiceException e) {
                    e.printStackTrace();
                }
                System.out.println("删除目录层级成功");
            }
        }
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
