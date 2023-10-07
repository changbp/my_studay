import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Date;
import java.util.*;

/**
 * @Author changbp
 * @Date 2021-04-25 9:58
 * @Return
 * @Version 1.0
 */
public class HBaseTest {
    private static final Logger log = LoggerFactory.getLogger(HBaseTest.class);

    public static void main(String[] args) {
        try {
            System.out.println(new Date());
            Connection conn = getConnection();
            assert conn != null;
            PreparedStatement preparedStatement = conn.prepareStatement("select * from \"a14e111af14b4959a5c06752dfbbeecf\".\"dt_member\" where CERTIFICATE_NUM = ?");
            preparedStatement.setString(1, "110224197806252223");
            ResultSet resultSet = preparedStatement.executeQuery();
            String result = JSON.toJSONString(getData(resultSet),
                    SerializerFeature.PrettyFormat,
                    SerializerFeature.WriteMapNullValue,
                    SerializerFeature.WriteDateUseDateFormat);
            System.out.println(result);
            resultSet.close();
            preparedStatement.close();
            conn.close();
            System.out.println(new Date());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 归集查询后的数据并注入到 List
     *
     * @param resultSet
     * @return
     * @throws Exception
     */
    public static List<Map<String, Object>> getData(ResultSet resultSet) throws Exception {
        List<Map<String, Object>> list = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnSize = metaData.getColumnCount();

        while (resultSet.next()) {
            Map<String, Object> map = new HashMap<>();
            for (int i = 1; i < columnSize + 1; i++) {
                map.put(metaData.getColumnLabel(i), resultSet.getObject(i));
            }
            list.add(map);
        }
        return list;
    }

    /*
    * 创建 phoenix 连接
    * 适配开启 Kerberos 认证的hbase集群
    * */
    private static Connection getConnection() {
        try {
            Properties properties = new Properties();
            //相关hbase配置
            properties.setProperty("hbase.zookeeper.quorum", "192.168.1.211,192.168.1.212,192.168.1.213");
            properties.setProperty("hbase.master.kerberos.principal", "hadoop/hdfs01@BDSOFT.COM");
            properties.setProperty("hbase.regionserver.kerberos.principal", "hadoop/hdfs01@BDSOFT.COM");
            properties.setProperty("phoenix.queryserver.kerberos.principal", "hadoop/hdfs01@BDSOFT.COM");
            properties.setProperty("hbase.security.authentication", "kerberos");
            properties.setProperty("hadoop.security.authentication", "kerberos");
            properties.setProperty("zookeeper.znode.parent", "/hbase");
            //个人的用户验证文件  配置方式添加 principal 和 keytab
            properties.setProperty("hbase.myclient.principal","hadoop/hdfs01@BDSOFT.COM");
            properties.setProperty("hbase.myclient.keytab","D:\\hadoop.keytab");
            System.setProperty("java.security.krb5.conf","D:\\krb5.conf");
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            //也可以直接再url 拼接 principal 和 keytab
//          String url = "jdbc:phoenix:192.168.1.211,192.168.1.212,192.168.1.213:2181:/hbase:hadoop/hdfs01@BDSOFT.COM:D:\\hadoop.keytab";
            String url = "jdbc:phoenix:192.168.1.211,192.168.1.212,192.168.1.213:2181:/hbase";
            Connection connection = DriverManager.getConnection(url, properties);
            return connection;
        } catch (Exception e) {
            System.out.println("连接phoenix异常");
            e.printStackTrace();
            return null;
        }
    }
}
