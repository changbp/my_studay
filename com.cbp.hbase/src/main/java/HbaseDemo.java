import com.sun.codemodel.internal.JForEach;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.UUID;

/**
 * @Author changbp
 * @Date 2021-04-14 17:03
 * @Return  hbase 2.X 代码适配
 * @Version 1.0
 */
public class HbaseDemo {
    private static String ZKIP = "host233";
    private static String PORT = "2181";
    private static String ZNODE = "/hbase";

    public static void main(String[] args) {
        String tablename = "f2b79ae159fe454e92b4ff7fc49b48d3:mygame001";
        String partition = "2";
//        makeTable(tablename, partition);
//        deleteTable(tablename);
//        getTableRowNum(tablename);
//        String s = HbaseUitls.tableData(ZKIP, PORT, tablename, 100);
        Connection connction = HbaseUitls.getConnction(ZKIP, PORT);
        try {
            NamespaceDescriptor[] namespaceDescriptors = connction.getAdmin().listNamespaceDescriptors();
            for(NamespaceDescriptor namespaceDescriptor : namespaceDescriptors){
                System.out.println(namespaceDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    /*
     * 测试创建表
     * */
    public static void makeTable(String tablename, String partition) {
        String namespace = UUID.randomUUID().toString().replace("-", "");
        try {
            String hbaseTable = HbaseUitls.createHbaseTable(tablename, namespace, ZKIP, PORT, ZNODE, partition);
            System.out.println("hbase创建成功===========" + hbaseTable);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * 测试删除hbase表
     * */
    public static void deleteTable(String tablename) {
        try {
            Boolean flag = HbaseUitls.deleteTable(ZKIP, PORT, ZNODE, tablename);
            System.out.println("hbase表删除成功===========" + flag);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    /*
     * 测试创建表
     * */
    public static void getTableRowNum(String tablename) {
        String namespace = UUID.randomUUID().toString().replace("-", "");
        try {
            long tableRowNum = HbaseUitls.getTableRowNum(ZKIP, PORT, ZNODE, tablename);
            System.out.println("hbase创建成功===========" + tableRowNum);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

}
