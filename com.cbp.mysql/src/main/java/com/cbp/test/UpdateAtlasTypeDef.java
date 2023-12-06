package com.cbp.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.typedef.AtlasTypesDef;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/12/1 11:57
 */
public class UpdateAtlasTypeDef {
    public static void main(String[] args) {
        try {
            AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://132.35.231.159:21000/"}, new String[]{"admin", "!QAZ2wsx3edc"});
            File file = new File("D:\\rdbms\\upLine\\0010-base_model.json");
            String jsonStr = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
            AtlasTypesDef atlasTypesDef = JSONObject.parseObject(jsonStr, AtlasTypesDef.class);
//            atlasClientV2.deleteAtlasTypeDefs(atlasTypesDef);
            atlasClientV2.updateAtlasTypeDefs(atlasTypesDef);
//            atlasClientV2.createAtlasTypeDefs(atlasTypesDef);
        } catch (AtlasServiceException | IOException e) {
            e.printStackTrace();
        }
    }
}
