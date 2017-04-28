package com.zks.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class TextDemo {

	private Configuration config = null;

	@Before
	public void init() {
		try {

			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum",
					"hdp01-2.ssii.com.cn:2181,hdp01-3.ssii.com.cn:2181,hdp01-1.ssii.com.cn:2181");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * ��������
	 */
	@Test
	public void testAddData() {
		try {
			HTable table = new HTable(config, "student");
			Put put = new Put("zks001".getBytes());
			put.add("common".getBytes(), "age".getBytes(), "testzks0001".getBytes());
			table.put(put);
			table.close();
			System.out.println("�������ݳɹ�");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * ������
	 * 
	 * @throws IOException
	 */
	@Test
	public void createTable() throws IOException {
		String tableName = "people";
		Connection conn = ConnectionFactory.createConnection(config);
		Admin admin = conn.getAdmin();
		try {

			if (admin.tableExists(TableName.valueOf(tableName))) {
				System.out.println("���ڱ���Ϊ��" + tableName + " �ı�");
				return;
			}
			TableName table = TableName.valueOf(tableName);
			HTableDescriptor tableDesc = new HTableDescriptor(table);
			tableDesc.addFamily(new HColumnDescriptor("common"));
			tableDesc.addFamily(new HColumnDescriptor("special"));
			admin.createTable(tableDesc);
			System.out.println(tableName + "�����ɹ�");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			admin.close();
			conn.close();
		}
	}

	/**
	 * ��������
	 */
	@Test
	public void addDataTwo() throws IOException {
		String tableName = "people";
		Connection conn = ConnectionFactory.createConnection(config);
		Table table = conn.getTable(TableName.valueOf(tableName));
		try {
			Put put = new Put(Bytes.toBytes("zks001" + (int) Math.random() * 1000));
			put.addColumn("common".getBytes(), "name".getBytes(), "zks".getBytes());
			table.put(put);
			System.out.println("�������ݳɹ�");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			conn.close();
		}
	}

	/**
	 * Ϊһ���Ѿ����ڵı����һ������ �˷�������һ��bug ��ĳ���������ʱ��������Ӹ����壬�ᱨ��
	 * 
	 * @throws IOException
	 */
	@Test
	public void putFamily() throws IOException {
		Connection conn = ConnectionFactory.createConnection(config);
		Admin admin = conn.getAdmin();
		try {
			TableName tableName = TableName.valueOf("people");
			if (!admin.tableExists(tableName)) {
				System.out.println("�����ڱ�people");
				return;
			}
			admin.disableTable(tableName);
			HColumnDescriptor columnDesc = new HColumnDescriptor("veryspecial");
			admin.addColumn(tableName, columnDesc);
			admin.enableTable(tableName);
			System.out.println("�������ɹ�");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			admin.close();
			conn.close();
		}
	}

	/**
	 * @throws IOException
	 *             ͨ��rowkey ����ȡ����
	 */
	@Test
	public void getDataByKey() throws IOException {
		Connection conn = ConnectionFactory.createConnection(config);
		Table table = conn.getTable(TableName.valueOf("people"));
		try {
			Get get = new Get("zks0011.3010553314539586".getBytes());
			Result result = table.get(get);
			byte[] value = result.getValue("common".getBytes(), "name".getBytes());
			if (value == null || value.length <= 0) {
				System.out.println("������");
				return;
			}
			String name = new String(value, "UTF-8");
			System.out.println(name);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			table.close();
			conn.close();
		}
	}
	/**
	 * scan all data
	 * @throws IOException 
	 */
	@Test
	public void scanAll() throws IOException{
		Connection conn=ConnectionFactory.createConnection(config);
		Table table=conn.getTable(TableName.valueOf("people"));
		try {
			Scan scan=new Scan();
			ResultScanner resultScanner=table.getScanner(scan);
			for(Result result:resultScanner){
				List<Cell> listCells = result.listCells();
				for(Cell cell:listCells){
					String rowkey=new String(result.getRow(),"UTF-8");
					
					
					System.out.println(rowkey);
				}
			}
			
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			table.close();
			conn.close();
		}
	}
	
}
