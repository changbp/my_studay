import java.sql.*;

/**
 * @Author changbp
 * @Date 2021-04-25 16:07
 * @Return
 * @Version 1.0
 */
public class PhoenixTest {
    private Connection conn;
    private PreparedStatement stat;
    private ResultSet resultSet;

    public void initResource() throws Exception {
        Class.forName(PhoenixUtils.getDriver());
        conn = DriverManager.getConnection(PhoenixUtils.getUrl(), PhoenixUtils.getUserName(), PhoenixUtils.getPassWord());
    }

    public void testSelectTable() throws SQLException {
        String sql = "select * from \"ae163cf43c304a7593df08bf4d0dd9f8\".\"au04\"";
        stat = conn.prepareStatement(sql);
        resultSet = stat.executeQuery(sql);
        while (resultSet.next()) {
            int n = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= n; i++) {
                System.out.println(resultSet.getMetaData().getColumnName(i) + "：" + resultSet.getString(resultSet.getMetaData().getColumnName(i)));
            }
            System.out.println("******************************");
        }
        conn.commit();
    }

    public void testCreateTable() throws SQLException {
        String sql = "create table test_phoenix_api(mykey integer not null primary key ,mycolumn varchar )";
        stat.executeUpdate(sql);
        conn.commit();
    }

    public void upsert() throws SQLException {
        String sql1 = "upsert into test_phoenix_api values(1,'test1')";
        String sql2 = "upsert into test_phoenix_api values(2,'test2')";
        String sql3 = "upsert into test_phoenix_api values(3,'test3')";
        stat.executeUpdate(sql1);
        stat.executeUpdate(sql2);
        stat.executeUpdate(sql3);
        conn.commit();
    }

    public void delete() throws SQLException {
        String sql1 = "delete from test_phoenix_api where mykey = 1";
        stat.executeUpdate(sql1);
        conn.commit();
    }


    public void closeResource() throws SQLException {
        if (resultSet != null) {
            resultSet.close();
        }
        if (stat != null) {
            stat.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
