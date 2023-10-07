import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.commons.dbcp.BasicDataSource;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @Author changbp
 * @Date 2021-06-24 9:26
 * @Return
 * @Version 1.0
 */
public class MySqlTest {
    static BasicDataSource dataSource = new BasicDataSource();

    static {
        //dbcp数据库连接池
        /* Mysql Server 必须要的配置 */
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://192.168.1.215:3306?useSSL=false");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        dataSource.setDefaultCatalog("test_sl");
        //可选配置
        dataSource.setMaxActive(10);//连接池最大连接数
        dataSource.setMaxIdle(5);//连接池最大空闲数
        dataSource.setMinIdle(3);//连接池最小空闲数
        dataSource.setInitialSize(10);//初始化连接池时的连接数

        /*            //HikariCP数据库连接池
            final HikariConfig hikariConfig = new HikariConfig();
            *//* Mysql Server 必须要的配置 *//*
            hikariConfig.setDriverClassName("com.mysql.jdbc.Driver");
            hikariConfig.setJdbcUrl("jdbc:mysql://192.168.1.215:3306");
            hikariConfig.setUsername("root");
            hikariConfig.setPassword("123456");
            hikariConfig.setCatalog("ddg_dabotkg");
            //可选配置
            hikariConfig.setMaximumPoolSize(10);//连接池最大连接数
            hikariConfig.setConnectionTimeout(10000);//连接超时时间 毫秒
            hikariConfig.setMaxLifetime(60000);//空闲超时 毫秒
//            hikariConfig.addDataSourceProperty("connectionTimeout", "1000");
//            hikariConfig.addDataSourceProperty("idleTimeout", "60000");
//            hikariConfig.addDataSourceProperty("maximumPoolSize", "10");
            final DataSource hikariDataSource = new HikariDataSource(hikariConfig);*/
    }

    public static void main(String[] args) {
        //获取数据
        String sql = "select * from test_sl.billion_data01 limit 1,10";
        final String data = operateData(sql, 1);
        System.out.println(data);
//        try {
//            insert();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        } catch (SQLException throwables) {
//            throwables.printStackTrace();
//        }
//        System.out.println(data);

//        String createSql = "CREATE TABLE IF NOT EXISTS  au01( AU00 varchar(36), AU0101 varchar(100))";
//        final String data1 = operateData(createSql, 2);
//        System.out.println(data1);
    }


    /*
     * 操作数据
     * */
    public static String operateData(String sql, int flag) {
        final JSONObject resObj = new JSONObject();
        String result = null;
        Properties info = new Properties();
        info.setProperty("lex", "MYSQL");//策略为MYSQL
        info.setProperty("conformance", "MYSQL_5");//开启支持limit 1,10
        info.setProperty("caseSensitive", "false");//不区分大小写
        info.setProperty("parserFactory", "org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl#FACTORY");//开启支持ddl 类型语句
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
            Class.forName("org.apache.calcite.jdbc.Driver");
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            JdbcSchema jdbcSchema = JdbcSchema.create(rootSchema, dataSource.getDefaultCatalog(), dataSource, null, dataSource.getDefaultCatalog());
            rootSchema.add(dataSource.getDefaultCatalog(), jdbcSchema);

            //查询所有表
            final Set<String> tableNames = jdbcSchema.getTableNames();
            for(String str : tableNames){
                System.out.println(str);
            }

            //获取表的类型  视图或者表
            Table table = jdbcSchema.getTable("user01");
            final Schema.TableType jdbcTableType = table.getJdbcTableType();
            final String name = jdbcTableType.name();
            System.out.println("===="+name);


            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                if (flag == 1) {
                    try (ResultSet resultSet = ps.executeQuery()) {
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        int columnSize = metaData.getColumnCount();
                        JSONArray showData = new JSONArray();
                        while (resultSet.next()) {
                            JSONObject rowObj = new JSONObject();
                            Map<String, Object> map = new HashMap<>();
                            for (int i = 1; i < columnSize + 1; i++) {
                                rowObj.put(metaData.getColumnLabel(i), resultSet.getObject(i));
                            }
                            showData.add(rowObj);
                        }
                        resObj.put("data", showData);
                    }
                }
//                Thread.sleep(1000);
                if (flag == 2) {
                    result = String.valueOf(ps.executeUpdate());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resObj.toJSONString();
    }

    public static void insert() throws ClassNotFoundException, SQLException {

        Class.forName("org.apache.calcite.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        JdbcSchema jdbcSchema =
                JdbcSchema.create(rootSchema, dataSource.getDefaultCatalog(), dataSource, null, null);
        rootSchema.add(dataSource.getDefaultCatalog(), jdbcSchema);

        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        configBuilder.defaultSchema(rootSchema);

        FrameworkConfig frameworkConfig = configBuilder.build();

        SqlParser.ConfigBuilder paresrConfig = SqlParser.configBuilder(frameworkConfig.getParserConfig());

        paresrConfig.setCaseSensitive(false).setConfig(paresrConfig.build());

        Planner planner = Frameworks.getPlanner(frameworkConfig);

        SqlNode sqlNode = null;
        RelRoot relRoot = null;
        try {
            String sql = "select * from \"test_sl\".\"user01\"";
            sqlNode = planner.parse(sql);
            planner.validate(sqlNode);
            relRoot = planner.rel(sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
        }

        RelNode relNode = relRoot.project();
        System.out.print(RelOptUtil.toString(relNode));

    }
}
