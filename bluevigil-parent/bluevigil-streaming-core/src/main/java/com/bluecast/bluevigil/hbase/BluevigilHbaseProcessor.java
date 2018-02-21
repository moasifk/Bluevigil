package com.bluecast.bluevigil.hbase;

import java.io.IOException;
import java.util.List;
//import java.sql.Connection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.bluecast.bluevigil.utils.Utils;
import com.google.protobuf.ServiceException;



	public class BluevigilHbaseProcessor {
		/*public static void main(String[] args) throws IOException, ServiceException {
			BluevigilHbaseProcessor bhp=new BluevigilHbaseProcessor();
			bhp.getHbaseData();
		}*/

	public  void getHbaseData() throws IOException, ServiceException{
		Connection con=Utils.getHbaseConnection();
		
		/*Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "nn02.itversity.com,nn01.itversity.com");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent","/hbase-unsecure");		
		Connection con=ConnectionFactory.createConnection(conf);*/
		TableName tn=TableName.valueOf("BlueVigilHistory");
		Table tbl=con.getTable(tn);
		Scan scan=new Scan();
		ResultScanner rs=tbl.getScanner(scan);
		System.out.println("Connected to hbase");
		for(Result res:rs) {
			
	        for(Cell cell: res.listCells()) {
	            byte[] family=cell.getFamily();
	            byte[] qual=cell.getQualifier();
	            System.out.println(new String(family)+" and  "+new String(qual)+" value= "+new String(res.getValue(family,qual)));
	        }
		}
		
		//updating Htbale row 'row1'
		
		Put p = new Put(Bytes.toBytes("row3"));
		p.add(Bytes.toBytes("Http Field Mapping"),Bytes.toBytes("uid,"),Bytes.toBytes("CakRoy36M9U1oqIu1f"));
		p.add(Bytes.toBytes("Http Field Mapping"),Bytes.toBytes("ts,"),Bytes.toBytes("1510665263.924683"));
		tbl.put(p);
		System.out.println("Data is inserted");
	

	            
	}
	
}
