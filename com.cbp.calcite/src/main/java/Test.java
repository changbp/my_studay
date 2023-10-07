import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.util.Sources;
import org.apache.commons.dbcp.BasicDataSource;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @Author changbp
 * @Date 2021-06-24 15:50
 * @Return
 * @Version 1.0
 */
public class Test {
    public static void main(String[] args) {
        System.out.println("start======================"+new SimpleDateFormat("yyyy-HH-dd hh:mm:ss:SSS").format(new Date()));
        final String path = Sources.of(Test.class.getResource("demo.json")).file().getAbsolutePath();
        System.out.println(path);
        Properties config = new Properties();
        config.setProperty("model", path);//引入schema json
        config.setProperty("lex", "MYSQL");//策略为MYSQL
        config.setProperty("conformance", "MYSQL_5");//开启支持limit 1,10
        config.setProperty("caseSensitive", "false");//不区分大小写
        config.setProperty("parserFactory", "org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl#FACTORY");//开启支持ddl 类型语句
//        String sql = "select * from MYSQL.billion_data01 where f1 = '28140eb0-c607-48a5-b18c-6e4e310baa33' ";
        String sql = "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA";
        String sql1 =  "select table_name from information_schema.tables where table_schema='test_sl' ";
        String createSql = "CREATE TABLE t (i INTEGER, j VARCHAR(10))";

        try (Connection con = DriverManager.getConnection("jdbc:calcite:", config)) {
            try (Statement stmt = con.createStatement()) {
                final boolean execute = stmt.execute(createSql);
                System.out.println(execute);
////                final int i = stmt.executeUpdate(sql1);
//                try (ResultSet rs = stmt.executeQuery(sql1)) {
//                    final String res = JSON.toJSONString(CalciteUtil.getData(rs),
//                            SerializerFeature.PrettyFormat,
//                            SerializerFeature.WriteMapNullValue,
//                            SerializerFeature.WriteDateUseDateFormat);
//                    System.out.println(res);
//                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("end======================"+new SimpleDateFormat("yyyy-HH-dd hh:mm:ss:SSS").format(new Date()));
    }
}
