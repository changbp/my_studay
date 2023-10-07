package day_test;

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
        Map<String, Map<String, Object>> data = new HashMap<>();
        Map<String, Object> map = new HashMap<>();
        map.put("entity","111");
        map.put("general","111");
        map.put("system","111");
        data.put("data",map);
        System.out.println(data);
        data.get("data").remove("system");
        System.out.println(data);
    }
}
