package com.xyxy.data.utils; /**
 * @Author Jacky Zou
 * @Date 2022/8/27 12:37
 * @Version 1.0
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtils {

    Connection connection = null;

    private HBaseUtils(){
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","ubuntu20");
        configuration.set("hbase.rootdir","hdfs://ubuntu20:9000/hbase");

        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;

    public static synchronized HBaseUtils getInstance(){
        if(null == instance){
            instance = new HBaseUtils();
        }
        return instance;
    }
    /**
     * 根据表名获取到 Htable 实例
     */

    public Table getTable(String tableName){

        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }
    /**
     * 添加一条记录到 Hbase 表 70 30 128 32 核 200T 8000
     * @param tableName Hbase 表名
     * @param rowkey Hbase 表的 rowkey * @param cf Hbase 表的 columnfamily * @param column Hbase 表的列
     * @param value 写入 Hbase 表的值
     */
    public void put(String tableName,String rowkey,String cf,String column,String value){
        Table table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(table != null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) {
        //HTable table = com.xyxy.data.utils.com.xyxy.data.utils.HBaseUtils.getInstance().getTable("category_clickcount");
        //System.out.println(table.getName().getNameAsString());
        String tableName = "student";
        String rowkey = "003";
        String cf="info";
        String column ="name";
        String value = "邹泽远";
        HBaseUtils.getInstance().put(tableName,rowkey,cf,column,value);
    }
}
