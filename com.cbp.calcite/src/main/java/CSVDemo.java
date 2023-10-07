import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;

/**
 * @Author changbp
 * @Date 2021-06-22 10:35
 * @Return
 * @Version 1.0
 */
public class CSVDemo {
    public static void main(String[] args) {
        try {
            String sql = "select u.id as user_id, u.name as user_name, j.company as user_company, u.age as user_age  " +
                    "from users u join jobs j on u.name=j.name " +
                    "where u.age > 30 and j.id>10 " +
                    "order by user_id";
            final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("USERS", new AbstractTable() {
                @Override
                public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                    RelDataTypeFactory.Builder builder = typeFactory.builder();
                    builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {
                    }, SqlTypeName.INTEGER));
                    builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {
                    }, SqlTypeName.CHAR));
                    builder.add("AGE", new BasicSqlType(new RelDataTypeSystemImpl() {
                    }, SqlTypeName.INTEGER));
                    return builder.build();
                }
            });
            rootSchema.add("JOBS", new AbstractTable() {
                @Override
                public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                    RelDataTypeFactory.Builder builder = typeFactory.builder();

                    builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {
                    }, SqlTypeName.INTEGER));
                    builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {
                    }, SqlTypeName.CHAR));
                    builder.add("COMPANY", new BasicSqlType(new RelDataTypeSystemImpl() {
                    }, SqlTypeName.CHAR));
                    return builder.build();
                }
            });
            SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
            SqlNode sqlNode = parser.parseStmt();
            System.out.println("优化前=============" + sql);
            System.out.println("优化后=============" + sqlNode);
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
    }
}
