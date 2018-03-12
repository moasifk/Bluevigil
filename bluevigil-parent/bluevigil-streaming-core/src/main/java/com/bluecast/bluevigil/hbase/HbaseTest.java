package com.bluecast.bluevigil.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HbaseTest {
	/*public static void main(String[] args) {
		String tableName="MyTable";
		String columnFamily="MyTableFamily";
		isHbaseTableExists(tableName, columnFamily);
	
	}*/
	
	public static  void isHbaseTableExists(String hbaseTableName,String hbaseTableColumnFamily) {
		//Connection con=new Conne
		Configuration conf=HBaseConfiguration.create();
		
		/*conf.set("hbase.zookeeper.quorum", "nn02.itversity.com,nn01.itversity.com");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent","/hbase-unsecure");	*/
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent","/hbase-unsecure");	
		try {
		HBaseAdmin hba = new HBaseAdmin(conf);
		
			if(!hba.tableExists(hbaseTableName)){
				
				HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
			    HColumnDescriptor columnDescriptor = new HColumnDescriptor(hbaseTableColumnFamily);
			    tableDescriptor.addFamily(columnDescriptor);
			    System.out.println("In Hbase table creatioin");
			    hba.createTable(tableDescriptor);
			    System.out.println("Hbase table "+hbaseTableName+" has been created");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
