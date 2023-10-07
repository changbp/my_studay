import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @Author changbp
 * @Date 2021-04-14 17:02
 * @Return
 * @Version 1.0
 */
public class HbaseUitls {
    private static final Logger log = LoggerFactory.getLogger(HbaseUitls.class);

    /*
     * 预览数据
     * */
    public static String tableData(String masterIp, String hbasePort, String tableName, Integer batchSize) {

        JSONArray resArray = new JSONArray();
        Connection connection = null;
        Table table = null;
        ResultScanner rscan = null;
        try {
            //创建连接
            connection = getConnction(masterIp, hbasePort);
            //获取表   表名
            TableName tname = TableName.valueOf(tableName);
            //获取Tables
            table = connection.getTable(tname);
            //获取前100条数据
            Scan scan = new Scan();
            Filter pageFilter = new PageFilter(batchSize);
            scan.setFilter(pageFilter);
            rscan = table.getScanner(scan);
            JSONObject showObj = new JSONObject();
            for (Result row : rscan) {
                System.out.println("-------------------------------");
                System.out.println("rowKey:" + Bytes.toString(row.getRow()));
                Cell[] cells = row.rawCells();
                for (Cell c : cells) {
                    String columnName = Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
                    String columnValue = null;
                    if (columnName.equals("GAME") || columnName.equals("ID") || columnName.equals("MONEY") || columnName.equals("MONEY_negate") || columnName.equals("TYPE")) {
                        int result = Bytes.toInt(c.getValueArray());
                        columnValue = Integer.toString(result);
                    } else {
                        columnValue = Bytes.toString(c.getValueArray(), c.getValueOffset(), c.getValueLength());
                    }
                    System.out.println(columnName + ":" + columnValue);
                    showObj.put(columnName, columnName + ":" + columnValue);
                    resArray.add(showObj);
                }
            }
            return resArray.toJSONString();
        } catch (Exception e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
            return null;

        } finally {
            if (rscan != null) {
                rscan.close();
            }
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 写入数据
     *
     * @param zkIp
     * @param zkPort
     * @param hbaseZNode
     * @param name
     * @return
     */
    public static Boolean insertHbase(String zkIp, String zkPort, String hbaseZNode, String name) throws Throwable {

        Boolean flag = false;
        Connection connection = null;
        try {
            connection = getConnction(zkIp, zkPort);
            TableName tableName = TableName.valueOf(name);
            Table table = connection.getTable(tableName);
            Put put = new Put("row2".getBytes());
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes("2"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes("3"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes("4"));
            table.put(put);
            flag = true;
            return flag;
        } catch (Exception e) {
            log.error(e.getMessage());
            return flag;
        } finally {
            log.info("关闭hbase连接");
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * 获取hbase中表的记录数 , 此方法查询记录数较快 可到秒级
     *
     * @param zkIp
     * @param zkPort
     * @param hbaseZNode
     * @param tablename
     * @return
     */
    public static long getTableRowNum(String zkIp, String zkPort, String hbaseZNode, String tablename) throws Throwable {
        long RowCount = 0;
        Connection connection = null;
        Admin admin = null;
        AggregationClient aggregationClient = null;
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkIp);
        conf.set("hbase.zookeeper.property.clientPort", zkPort);
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tablename);

            //先disable表，添加协处理器后再enable表
            admin.disableTable(tableName);
            TableDescriptor descriptor = admin.getDescriptor(tableName);
            String coprocessorClass = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
            if (!descriptor.hasCoprocessor(coprocessorClass)) {
                descriptor.getCoprocessorDescriptors();
            }
            admin.modifyTable(descriptor);
            admin.enableTable(tableName);
            Scan scan = new Scan();
            aggregationClient = new AggregationClient(conf);
            RowCount = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
        } catch (Exception e) {
            log.error("获取hbase记录数：" + e.getMessage());
        } finally {
            log.info("关闭hbase连接");
            if (connection != null) {
                connection.close();
            }
            if (aggregationClient != null) {
                aggregationClient.close();
            }
        }
        return RowCount;
    }

    /**
     * 初始化hbase表
     *
     * @param zkIp
     * @param zkPort
     * @param hbaseZNode
     * @param tablName
     * @return
     */
    public static Boolean deleteTable(String zkIp, String zkPort, String hbaseZNode, String tablName) throws Throwable {

        Boolean flag = false;
        Connection connction = null;
        Admin admin = null;
        try {
            connction = getConnction(zkIp, zkPort);
            admin = connction.getAdmin();
            TableName tableName = TableName.valueOf(tablName);
            if (admin.tableExists(tableName)) {
                //先disable禁用表，删除表
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                flag = true;
            }
            return flag;
        } catch (Exception e) {
            log.error(e.getMessage());
            return flag;
        } finally {
            log.info("关闭hbase连接");
            //释放资源
            if (admin != null) {
                admin.close();
            }
            if (connction != null) {
                connction.close();
            }
        }
    }

    /****
     * 新建hbase表
     * @param hbaseTableName
     */
    public static String createHbaseTable(String hbaseTableName, String nameSpace, String zookeeperIp, String zookeeperPort, String hbaseznode, String partitions) throws IOException {
        //hbase创建表返回值信息
        JSONObject createTableInfo = new JSONObject();
        Connection connction = null;
        Admin admin = null;
        try {
            connction = getConnction(zookeeperIp, zookeeperPort);
            admin = connction.getAdmin();
            createTableInfo.put("isSuccess", true);
            NamespaceDescriptor[] nameList = admin.listNamespaceDescriptors();
            //namespace是否存在 0 不存在
            String namespaceFlag = "0";
            for (NamespaceDescriptor namespaceDesc : nameList) {
                if (namespaceDesc.getName().equals(nameSpace)) {
                    namespaceFlag = "1";
                }
            }
            if (namespaceFlag.equals("0")) {
                admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
            }
            TableName tableName = TableName.valueOf(nameSpace + ":" + hbaseTableName);
            if (admin.tableExists(tableName)) {
                //表存在删除表      暂时不做处理
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            } else {
                TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
                ColumnFamilyDescriptorBuilder columnDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("user"));
                //开启snappy压缩
                columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY).setCompactionCompressionType(Compression.Algorithm.SNAPPY);
                ColumnFamilyDescriptor familyDescriptor = columnDescriptor.build();
                //添加列族
                tableDescriptor.setColumnFamily(familyDescriptor);
                if (StringUtils.isEmpty(partitions)) {
                    partitions = "1";
                }
                byte[][] splitKeys = getSplitKeys(Integer.parseInt(partitions));
                admin.createTable(tableDescriptor.build(), splitKeys);
            }
        } catch (Exception e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
            System.out.println(ExceptionUtils.getFullStackTrace(e));
            createTableInfo.put("isSuccess", false);
            createTableInfo.put("message", "  创建HBase表失败，请和管理员联系！");
            return createTableInfo.toString();
        } finally {
            //释放资源
            if (admin != null) {
                admin.close();
            }
            if (connction != null) {
                connction.close();
            }
        }
        return createTableInfo.toString();
    }

    /****
     *
     * @param regionNum 分区数
     * @return
     */
    public static byte[][] getSplitKeys(int regionNum) {

        List<String> keysList = getSplitForRadix(regionNum, 10, "00", "99");
        String[] keysStr = new String[keysList.size()];
        byte[][] splitKeys = new byte[keysStr.length][];
        for (int i = 0; i < keysList.size(); i++) {
            keysStr[i] = keysList.get(i) + "|";
        }

        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keysStr.length; i++) {
            String keyStr = keysStr[i].length() == 2 ? "0" + keysStr[i] : keysStr[i];
            rows.add(Bytes.toBytes(keyStr));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }


        return splitKeys;
    }

    /***
     *
     * @param region 分区数
     * @param radix 10，16进制
     * @param start 开始数
     * @param end  结束数
     * @return
     */
    public static List<String> getSplitForRadix(int region, int radix, String start, String end) {
        Integer s = Integer.parseInt(start);
        Integer e = Long.valueOf(end, radix).intValue() + 1;
        return IntStream
                .range(s, e)
                .filter(value -> (value % ((e - s) / region)) == 0)
                .mapToObj(value -> {
                    if (radix == 16) {
                        return Integer.toHexString(value);
                    } else {
                        return String.valueOf(value);
                    }
                })
                .skip(1)
                .collect(Collectors.toList());
    }

    /*
     * 创建连接
     * */
    public static Connection getConnction(String zkIp, String port) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkIp);
        conf.set("hbase.zookeeper.property.clientPort", port);
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
