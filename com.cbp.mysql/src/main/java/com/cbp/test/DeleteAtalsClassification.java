package com.cbp.test;

import com.alibaba.fastjson.JSON;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;

import javax.ws.rs.core.MultivaluedMap;
import java.util.List;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/11/23 16:43
 */
public class DeleteAtalsClassification {
    public static void main(String[] args) {
        try {
            AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.159:21000/"}, new String[]{"admin", "!QAZ2wsx3edc"});
            MultivaluedMap<String, String> searchParams = new MultivaluedMapImpl();
            searchParams.add(SearchFilter.PARAM_TYPE, "CLASSIFICATION");
//            searchParams.add(SearchFilter.PARAM_NAME, "湖仓资产数据");
            SearchFilter searchFilter = new SearchFilter(searchParams);

            AtlasTypesDef allTypeDefs = atlasClientV2.getAllTypeDefs(searchFilter);
            System.out.println(JSON.toJSONString(allTypeDefs));
            List<AtlasClassificationDef> classificationDefs = allTypeDefs.getClassificationDefs();
            classificationDefs.forEach(f ->{
                try {
                    atlasClientV2.deleteTypeByName(f.getName());
                } catch (AtlasServiceException e) {
                    e.printStackTrace();
                }
            });
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
    }
}
