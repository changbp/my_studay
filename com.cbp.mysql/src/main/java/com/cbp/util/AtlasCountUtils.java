package com.cbp.util;

import com.alibaba.fastjson.JSON;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.metrics.AtlasMetrics;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

/**
 * @ProjectName: udme-bigdata-service
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/11/3 13:45
 */
public class AtlasCountUtils {
    private static final String ENTITYACTIVE = "entityActive";

    private static final String ENTITY = "entity";

    @Autowired
    private static AtlasClientV2 atlasClientV2;

    public static int getActiveEntityCount(String typeName) {
        try {
            AtlasMetrics atlasMetrics = atlasClientV2.getAtlasMetrics();
            Map<String, Map<String, Object>> data = atlasMetrics.getData();
            Map<String, Object> entity = data.get(ENTITY);
            Object activeCount = JSON.parseObject(JSON.toJSONString(entity.get(ENTITYACTIVE))).get(typeName);
            return Integer.parseInt(activeCount.toString());
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
        return 0;
    }

}
