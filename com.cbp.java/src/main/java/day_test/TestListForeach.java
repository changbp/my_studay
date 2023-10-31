package day_test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/8/31 17:48
 */
public class TestListForeach {
    public static void main(String[] args) {
//        ArrayList<String> classifications = new ArrayList<>();
////        classifications.add("1");
////        classifications.add("3");
////        classifications.add("4");
////        classifications.add("5");
////        StringBuilder catalogName = new StringBuilder();
////        classifications.forEach(line -> {
////            catalogName.append(line);
////            catalogName.append(",");
////        });
////        System.out.println(catalogName.substring(0,catalogName.length()-1));
//        Map<String, Map<String, Object>> data = new HashMap<>();
//        Map<String, Object> map = new HashMap<>();
//        map.put("entity","111");
//        map.put("general","111");
//        map.put("system","111");
//        data.put("data",map);
//        System.out.println(data);
//        data.get("data").remove("system");
//        System.out.println(data);
//        String datasourceConnParam = "'{\"id\":\"1\"}'";
//        String s = datasourceConnParam.replaceAll("'", "");
//        System.out.println(datasourceConnParam);
//        System.out.println(s);
//
//        Map<String, String> connParamMap = JSON.parseObject(s,
//                new TypeReference<HashMap<String, String>>() {});
//        System.out.println(connParamMap);
        String s = "******************************************************\nMon Oct 23 16:56:59 CST 2023：--------采集任务开始执行--------\n--------执行参数--------\n----job_name：ck_T2\n----job_group：DEFAULT\n----job_class：com.unicom.udme.gather.MyJob\n----cron_expression：null\n----job_data：{\"catalogName\":\"测试一下46654461654\",\"catalogGuid\":\"f56c4d69-c2d3-468b-863e-d19c9b35ff14\",\"createTime\":1698029361867,\"sourceSystem\":\"test\",\"dbName\":[\"default\"],\"id\":32,\"schemaName\":[[]],\"contentType\":\"application/json\",\"tableName\":[]}\n******************************************************\n----exception：null\n******************************************************\n--------任务执行成功--------\n--------运行时长1455ms\nMon Oct 23 16:57:01 CST 2023：--------采集任务结束执行--------\n";
        System.out.println(s);

    }
}
