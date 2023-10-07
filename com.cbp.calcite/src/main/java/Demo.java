import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.*;
import java.util.Properties;

/**
 * @Author changbp
 * @Date 2021-06-22 11:38
 * @Return
 * @Version 1.0
 */
public class Demo {
    public static class HrSchema {
//        public final Employee[] emps = 0;
//        public final Department[] depts = 0;
    }

    public static void main(String[] args) {
        Connection connection = null;
        ResultSet resultSet = null;
        Statement statement = null;
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
            Properties info = new Properties();
            info.setProperty("lex", "JAVA");
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            CalciteConnection calciteConnection =
                    connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            Schema schema = new ReflectiveSchema(new HrSchema());
            rootSchema.add("hr", schema);
            statement = calciteConnection.createStatement();
            resultSet = statement.executeQuery(
                    "select d.deptno, min(e.empid) "
                            + "from hr.emps as e "
                            + "join hr.depts as d "
                            + "  on e.deptno = d.deptno "
                            + "group by d.deptno "
                            + "having count(*) > 1");
            System.out.println(resultSet);
        } catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                resultSet.close();
                statement.close();
                connection.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }


    }
}
