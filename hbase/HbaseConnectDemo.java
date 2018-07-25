package com.hbase.lp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * hbase的java api访问
 * @author lp
 *
 */
public class HbaseConnectDemo{

	//声明静态配置
	private static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "mini2,mini3,mini4"); //
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		}
	
	
	
	//创建表
	public static void createTable(String tableName,String[] columnFamilys) throws Exception{
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();
		if(admin.tableExists(TableName.valueOf(tableName))){
			System.out.println(tableName+"表已经存在！");
		}else {
			//创建表的描述
			System.out.println("开始创建表");
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			//添加列族
			for (String columnFamily : columnFamilys){
				tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			}
			//创建表
			admin.createTable(tableDesc);
			System.out.println(tableName+"创建成功");
		}
		admin.close();
		
	}
	
	// 得到所有数据
    public static void getAllData(String tableName) throws Exception {  
        HTable table = new HTable(conf, tableName);  
        Scan scan = new Scan();  
        ResultScanner rs = table.getScanner(scan); 
        /**
         * 得到一行
         * Get get = new Get(Bytes.toBytes(row));
         * Result result = table.get(get);
         */
     // 遍历
        for (Result result : rs) {
            for (KeyValue rowKV : result.raw()) {
                System.out.print("行键:" + new String(rowKV.getKey()) + " ");
                System.out.print("时间戳:" + rowKV.getTimestamp() + " ");
                System.out.print("列族:" + new String(rowKV.getFamilyArray()) + " ");
                System.out
                        .print("列标识:" + new String(rowKV.getQualifierArray()) + " ");
                System.out.println("ֵ值:" + new String(rowKV.getValueArray()));
            }
        } 
    }
    //添加一条数据
    public static void addRow(String tableName, String row,
    		String columnFamily,String column, String value) throws IOException{
    	HTable table = new HTable(conf, tableName); 
    	Put put = new Put(Bytes.toBytes(row));
    	
    	put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
    	table.put(put);
    	System.out.println("添加成功！");
    }
    
    //删除一条数据
    public static void delRow(String tableName, String row) throws IOException{
    	HTable table = new HTable(conf, tableName); 
    	Delete del = new Delete(Bytes.toBytes(row));
    	table.delete(del);
    	System.out.println("删除成功！");
    	
    }
    //删除表
    public static void delTable(String tableName) throws IOException{
    	Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();
    	if(admin.tableExists(TableName.valueOf(tableName))){
    		//表不可用
    		admin.disableTables(tableName);
    		admin.deleteTables(tableName);
    		System.out.println("表 "+tableName+" 删除成功！");
    	}else{
    		System.out.println("表 "+tableName+" 不存在！");
            System.exit(0);
    	}
    	admin.close();
    }
    
    //插入200条数据
	public static void insertData(String tableName,String[] columnFamilys) throws IOException{
    	HTable table = new HTable(conf, tableName);
    	String bColumn = "dept_id";
    	String sColumn = "son_id";
    	String fColumn = "fat_id";
    	System.out.println("-------数据插入开始-------");
    	Put put1 = null;
    	Put put2 = null;
    	Put put3 = null;
    	Put put4 = null;
    	
    	//添加10个部门
    	System.out.println("****添加10个部门*****");
    	for (int i = 1; i <= 10; i++ ){
    		put3 = new Put(Bytes.toBytes("2015000"+i));
    		put3.addColumn(Bytes.toBytes(columnFamilys[0]), Bytes.toBytes(bColumn), Bytes.toBytes("20150"+i));
        	table.put(put3);
    	}
    	//为20150001添加100个子部门
    	System.out.println("****为20150001添加100个子部门*****");
    	for (int i = 1; i <= 100; i++ ){
    		put1 = new Put(Bytes.toBytes("20150001"));
    		put1.addColumn(Bytes.toBytes(columnFamilys[1]), Bytes.toBytes(sColumn+i), Bytes.toBytes("20150"+i));
        	table.put(put1);
    	}
    	//为9个部门添加父部门
    	System.out.println("****为9个部门添加父部门*****");
    	for (int i = 2; i <= 9; i++ ){
    		put4 = new Put(Bytes.toBytes("2015000"+i));
    		put4.addColumn(Bytes.toBytes(columnFamilys[1]), Bytes.toBytes(fColumn), Bytes.toBytes("20150001"));
        	table.put(put4);
    	}
    	//为20150100添加100个子部门
    	System.out.println("****为20150100添加100个子部门*****");
    	for (int i = 101, j = 0; i <= 200; i++, j++ ){
    		put2 = new Put(Bytes.toBytes("20150100"));
    		put2.addColumn(Bytes.toBytes(columnFamilys[1]), Bytes.toBytes(sColumn+j), Bytes.toBytes("20150"+i));
        	table.put(put2);
    	}
    	System.out.println("数据插入成功");
    }
    
    //查询
    public static void selectData(String tableName,String row) throws IOException{
    	HTable table = new HTable(conf, tableName);
    	Scan scan = new Scan();  
        ResultScanner rs = table.getScanner(scan); 
        //Get get = new Get(Bytes.toBytes(row));
        
        //查询所有一级部门
        /*System.out.println("------1.查询所有一级部门(父部门为空)------");
        //get.addColumn(Bytes.toBytes("base"),Bytes.toBytes("dept_id"));
        for (Result rt: rs){
        	if (rt.getValue(Bytes.toBytes("subdept"), Bytes.toBytes("fat_id")) == null){
                	System.out.println("行键：" + Bytes.toString(rt.getRow())+" "+"部门id："+
                			new String(rt.getValue(Bytes.toBytes("base"), Bytes.toBytes("dept_id"))));
        	}
        	
        }*/
        
        //已知rowkey，查询该部门的所有(直接)子部门信息
        /*System.out.println("------2.已知rowkey，查询该部门的所有(直接)子部门信息------");
        get.addFamily(Bytes.toBytes("subdept"));
        Result result = table.get(get);
        System.out.println("行键:" + new String(result.getRow()));
        for (Cell cell : result.rawCells()){
        	System.out.println("子部门id：" + Bytes.toString(CellUtil.cloneValue(cell)));
        }*/
         
        //已知rowkey，向该部门增加一个子部门
        /*System.out.println("------3.已知rowkey，向该部门增加一个子部门------");
        Put put = new Put(Bytes.toBytes(row));
		put.addColumn(Bytes.toBytes("subdept"), Bytes.toBytes("son_id101"), Bytes.toBytes("20150200"));
    	table.put(put);
    	System.out.println("------3.增加一个子部门成功------");*/
        //已知rowkey（且该部门存在子部门），删除该部门信息，该部门所有(直接)子部门被调整到其他部门中
        System.out.println("------4.已知rowkey，删除该部门信息------");
        System.out.println("4.1该部门所有(直接)子部门被调整到其他部门中");
        Get get = new Get(Bytes.toBytes("20150201"));
        Result rt = table.get(get);
        //得到该部门的子部门
        String son = new String(rt.getValue(Bytes.toBytes("subdept"), Bytes.toBytes("son_id")));
        //把该部门所有(直接)子部门被调整到其他部门中
        Put put = new Put(Bytes.toBytes(row));
		put.addColumn(Bytes.toBytes("subdept"), Bytes.toBytes("son_id102"), Bytes.toBytes(son));
    	table.put(put);
        System.out.println("4.1删除该部门信息");
        Delete del = new Delete(Bytes.toBytes("20150201"));
        table.delete(del);
        System.out.println("删除成功!");
        table.close();
       
    }
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String tableName="dept";
		String[]  columnFamilys = {"base","subdept"};
		//HbaseConnectDemo.createTable(tableName, columnFamilys);
		//HbaseConnectDemo.getAllData(tableName);
		//HbaseConnectDemo.addRow(tableName, "2015081125",columnFamilys[0], "name", "qy");
		//HbaseConnectDemo.delRow(tableName, "2015081125");
		//HbaseConnectDemo.delTable(tableName);
		//HbaseConnectDemo.insertData(tableName,columnFamilys);
		//HbaseConnectDemo.selectData(tableName, "20150001");
	}

}
