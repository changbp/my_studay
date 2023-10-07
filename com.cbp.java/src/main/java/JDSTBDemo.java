import com.alibaba.fastjson.JSONObject;

import java.util.Date;
import java.util.Set;

/**
 * @Author changbp
 * @Date 2021-06-01 11:21
 * @Return  json 属性类型获取
 * @Version 1.0
 */
public class JDSTBDemo {
    public static void main(String[] args) {
        JSONObject obj = new JSONObject();
        obj.put("id", 1);
        obj.put("name", "hello");
        obj.put("age", 12);
        obj.put("sex", '女');
        obj.put("money", 3600.8);
        obj.put("insertDate", new Date());
        Set<String> keys = obj.keySet();
        for (String key : keys) {
            Object value = obj.get(key);
            String name = value.getClass().getName();
            System.out.println(key + "========" + name);
        }


    }
}
