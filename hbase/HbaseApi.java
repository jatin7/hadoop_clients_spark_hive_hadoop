package com.hbase.lp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseApi {
	
	
	
	//声明静态配置
	static Configuration conf = null;
	static Connection connection = null;
	static{
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "mini2,mini3,mini4");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
			try {
				connection = ConnectionFactory.createConnection(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	}
	
	//通用初始化方法
	
	
	/**
	 * 创建表
	 * @param tableName
	 * @param columnFamilys
	 * @throws IOException
	 */
	public static void createTable(String tableName,String[] columnFamilys) throws IOException{
		Admin admin = connection.getAdmin();
		
		if(admin.tableExists(TableName.valueOf(tableName))){
			System.out.println(tableName+"表已经存在！");
			admin.close();
		}else{
			//建立表的描述
			System.out.println("------开始创建表-------");
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			//添加列族
			for (String family: columnFamilys){
				HColumnDescriptor hcd = new HColumnDescriptor(family);
				tableDesc.addFamily(hcd);
			}
			//创建表
			admin.createTable(tableDesc);
			System.out.println(tableName+"表创建成功！");
			admin.close();
		}
	}

	/**
	 * 删除表
	 * @param tableName
	 * @throws IOException
	 */
	public static void delTable(String tableName) throws IOException{
		Admin admin = connection.getAdmin();
		
		if(admin.tableExists(TableName.valueOf(tableName))){
			//表不可用
			admin.disableTables(tableName);
			//删除
			admin.deleteTables(tableName);
			System.out.println(tableName+"表删除成功！");
		}
		admin.close();
	}
	
	/**
	 * 修改表，删除三个列族，新增一个列族
	 * @param tableName
	 * @throws IOException
	 */
	public static void alertTable(String tableName) throws IOException{
		Admin admin = connection.getAdmin();
		
		if(admin.tableExists(TableName.valueOf(tableName))){
			admin.disableTables(tableName);
			//得到表的描述
			HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(tableName));
			//删除三个列族
			System.out.println("删除三个列族");
			tableDesc.removeFamily(Bytes.toBytes("note"));
			tableDesc.removeFamily(Bytes.toBytes("newcf"));
			tableDesc.removeFamily(Bytes.toBytes("sysinfo"));
			//添加一个新的列族
			
			HColumnDescriptor hcd = new HColumnDescriptor("action_log");
			hcd.setMaxVersions(10);
			//创建新的列族
			System.out.println("创建新的列族");
			tableDesc.addFamily(hcd);
			//修改表结构
			System.out.println("修改表结构");
			admin.modifyTable(TableName.valueOf(tableName), tableDesc);
			admin.enableTables(tableName);
			System.out.println("修改表成功!");
		}
		admin.close();
	}
	
	/**
	 * 插入数据
	 * @param tableName
	 * @throws IOException
	 */
	public static void insert(String tableName) throws IOException{
		Table table = connection.getTable(TableName.valueOf(tableName));  //代替HTable类
		System.out.println("添加信息");
		Put put = new Put(Bytes.toBytes("100001"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lion"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("adress"), Bytes.toBytes("shangdi"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("30"));
		table.put(put);
		System.out.println("添加成功！");
		table.close();
	}
	
	/**
	 * 得到一行数据
	 * @param tableName
	 * @param row
	 * @throws IOException
	 */
	public static void getRow(String tableName,String row) throws IOException{
		Table table = connection.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(row));
		Result rs = table.get(get);
		
		for (Cell cell: rs.rawCells()){
			System.out.println("行键:"+ Bytes.toString(rs.getRow()));
			System.out.println("列族名:列标识:"+ Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println("值:"+ Bytes.toString(CellUtil.cloneValue(cell)));
		}
		table.close();
		
	}
	
	/**
	 * 获取多行数据
	 * @param tableName
	 * @throws IOException
	 */
	public static void getAll(String tableName) throws IOException{
		Table table = connection.getTable(TableName.valueOf(tableName));
		
		Scan sc = new Scan();
		ResultScanner rs = table.getScanner(sc);
		
		for (Result r: rs){
			for (Cell cell: r.rawCells()){
				System.out.println("行键:"+ Bytes.toString(r.getRow()));
				System.out.println("列族名:列标识:"+ Bytes.toString(CellUtil.cloneFamily(cell))
				+ Bytes.toString(CellUtil.cloneQualifier(cell)));
				System.out.println("值:"+ Bytes.toString(CellUtil.cloneValue(cell)));
				System.out.println("时间戳:"+ cell.getTimestamp());
			}
		}
		
		table.close();
	}
	
	/**
	 * 列值过滤器
	 * @param tableName
	 * @throws IOException
	 */
	public static void filter(String tableName) throws IOException{
		Table table = connection.getTable(TableName.valueOf(tableName));
		FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		SingleColumnValueFilter sf1 = new SingleColumnValueFilter(
				Bytes.toBytes("info"),Bytes.toBytes("age"),CompareOp.GREATER_OR_EQUAL,
				Bytes.toBytes("25"));
		SingleColumnValueFilter sf2 = new SingleColumnValueFilter(
				Bytes.toBytes("info"),Bytes.toBytes("age"),CompareOp.LESS_OR_EQUAL,
				Bytes.toBytes("30"));
		fl.addFilter(sf1);
		fl.addFilter(sf2);
		sf1.setFilterIfMissing(true);//默认为false，没有此列的数据也会返回，为true则只返回age=25的数据
		Scan sc = new Scan();
		sc.setFilter(fl);
		ResultScanner rs = table.getScanner(sc);
		
		for (Result r: rs){
			for (Cell cell: r.rawCells()){
				System.out.println("行键:"+ Bytes.toString(r.getRow()));
				System.out.println("列族名:列标识:"+ Bytes.toString(CellUtil.cloneFamily(cell))
				+ Bytes.toString(CellUtil.cloneQualifier(cell)));
				System.out.println("值:"+ Bytes.toString(CellUtil.cloneValue(cell)));
				System.out.println("时间戳:"+ cell.getTimestamp());
			}
		}
		
		table.close();
	}
	
	/**
	 * 基于列族过滤数据的FamilyFilter
	 * @return
	 * @throws IOException
	 */
	public static ResultScanner familyFilter() throws IOException{
		Table table = connection.getTable(TableName.valueOf("user"));
		FamilyFilter ff = new FamilyFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("acount")));
		//表中不存在account列族，过滤结果为空  
		//       new BinaryPrefixComparator(value) //匹配字节数组前缀  
		//       new RegexStringComparator(expr) // 正则表达式匹配  
		//       new SubstringComparator(substr)// 子字符串匹配  
		Scan sc = new Scan();
		//通过scan.addFamily(family)也可以实现操作
		sc.setFilter(ff);
		ResultScanner rs = table.getScanner(sc);
		table.close();
		return rs;
	}
	
	/**
	 * 基于限定符Qualifier（列）过滤数据的QualifierFilter
	 * @throws IOException
	 */
	public static void qualifierFilter() throws IOException{
		Table table = connection.getTable(TableName.valueOf("user"));
		QualifierFilter qf = new QualifierFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("name")));
		//       new BinaryPrefixComparator(value) //匹配字节数组前缀  
		 //       new RegexStringComparator(expr) // 正则表达式匹配  
		 //       new SubstringComparator(substr)// 子字符串匹配   
		
		Scan sc = new Scan();
		//通过scan.addFamily(family)也可以实现操作
		sc.setFilter(qf);
		ResultScanner rs = table.getScanner(sc);
		table.close();
		
	}
	/**
	 * 基于列名(即Qualifier)前缀过滤数据的ColumnPrefixFilter
	 * 
	 */
	public static void prefixFilter(){}
	
	
	/**
	 * 基于多个列名(即Qualifier)前缀过滤数据的MultipleColumnPrefixFilter
	 * 
	 */
	public static void prefixFilters(){}
	
	/**
	 * 基于列范围过滤数据ColumnRangeFilter
	 * 
	 */
	public static void columnRangeFilter(){}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String tableName = "dept";
		String[] columnFamilys = {"info","note","newcf","sysinfo"};
		
		//HbaseApi.createTable(tableName, columnFamilys);
		//HbaseApi.delTable(tableName);
		//HbaseApi.alertTable(tableName);
		//HbaseApi.insert(tableName);
		//HbaseApi.getRow(tableName, "100001");
	}

}
