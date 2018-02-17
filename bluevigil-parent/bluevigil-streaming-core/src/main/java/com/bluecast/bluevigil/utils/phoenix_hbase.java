package com.bluecast.bluevigil.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;

 public class phoenix_hbase 
{
	public static void connectHbase() throws SQLException 
	{
		@SuppressWarnings("unused")
		Statement stmt = null;
		ResultSet rset = null;
	
		try 
		{
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		} 
		catch (ClassNotFoundException e1) 
		{
			System.out.println("Exception Loading Driver");
			e1.printStackTrace();
		}
		try
		{
			Connection con = DriverManager.getConnection("jdbc:phoenix:localhost:2181");  //172.31.124.43 is the adress of VM, not needed if ur running the program from vm itself
			stmt = con.createStatement();
				
			PreparedStatement statement = con.prepareStatement("select * from test");
			rset = statement.executeQuery();
			while (rset.next()) 
			{
				System.out.println(rset.getString("MYCLOUMN"));
			}
			statement.close();
			con.close();
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
}



























/*package com.bluecast.bluevigil.utils;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;



public class Test {
	public static void main(String[] args) throws IOException {
		IpResolveCountry ipc=new IpResolveCountry("./properties/GeoLite2-Country.mmdb");
		Tuple tuple= TupleFactory.getInstance().newTuple();//("162.168.1.1");
		tuple.append("157.48.67.70");
		String country=ipc.exec(tuple);
		System.out.println("Country ="+country);
		
		IpResolveCity ipCity=new IpResolveCity("./properties/GeoLite2-City.mmdb");
		Tuple cityTuple= TupleFactory.getInstance().newTuple();//("162.168.1.1");
		cityTuple.append("157.48.67.70");
		String city=ipCity.exec(cityTuple);
		System.out.println("Country ="+city);
		
	}

}*/
