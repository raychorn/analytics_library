package org.smithmicro.hive.server;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbcClient {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static String serverName = "hivedev1";
	private static String databaseName = "bakrie";
	
	/**
	 * @param args
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws SQLException {
	    // Check how many arguments were passed in
		if(args.length > 0)
	    {
			serverName = args[0];
			try {
				databaseName = args[1];
			} catch (ArrayIndexOutOfBoundsException e) {
				System.out.println("Using \"default\" database.");
			}
	    }
		// Get HiveConnectionManager
		HiveConnectionManager hiveManager = new HiveConnectionManager();
		
		String s = "select * from testHiveDriverTable";
		//Connection con = DriverManager.getConnection("jdbc:hive://hivedev1:10000/" + databaseName, "", "");
		Connection con = HiveConnectionManager.getConnection(serverName, "10000", databaseName);
		Statement stmt = con.createStatement();
		String tableName = "testHiveDriverTable";
		//stmt.executeQuery("use bakrie");
		hiveManager.useDatabase(con, "bakrie");
		// show database tables
                System.out.println("Tables in Database: '" + databaseName + "'");
                ResultSet resDb = stmt.executeQuery("show tables");
                while (resDb.next()) {
                        System.out.println(resDb.getString(1));
			// Describe Tables
			String sql = "describe " + resDb.getString(1);
                	System.out.println("Running: " + sql);
                	//ResultSet res = stmt.executeQuery(sql);
                	//while (res.next()) {
                        //	System.out.println(res.getString(1) + "\t" + res.getString(2));
                	//}
                }
		stmt.executeQuery("drop table " + tableName);
		ResultSet res = stmt.executeQuery("create table " + tableName + " (key int, value string)");
		// show tables
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}
		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}
		
		// load data into table
		// NOTE: filepath has to be local to the hive server
		String filepath = "/tmp/a.txt";
		sql = "load data local inpath '" + filepath + "' into table " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		
		// select * query
		sql = "select * from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
		}
		
		// regular hive query
    sql = "select count(1) from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}
	}
}
