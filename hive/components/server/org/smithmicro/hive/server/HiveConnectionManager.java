/*
 * Copyright (c) 2011. SmithMicro Software, Inc.
 * All Rights Reserved.
 */
package org.smithmicro.hive.server;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveConnectionManager {

	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	public static Connection getConnection(String theServer, String thePort, String theDatabase) 
								throws SQLException {
		try {
                Class.forName(driverName);
        } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
        }
		String connStr;
		String server = "hivedev1";
		String port = "10000";
		String database = "default";
		
		// Check parameters
		if(theServer != ""){
			server = theServer;
		}
		if(thePort != ""){
			port = thePort;
		}
		if(theDatabase != "") {
			database = theDatabase;
		}
		
		connStr = "jdbc:hive://" + server + ":" + port + "/" + database;
		return DriverManager.getConnection(connStr);		
	}
	
	public static void closeConnection(Connection conn) throws SQLException{
		conn.close();
	}
	
	// load data into a hive table
	public boolean loadData(Connection conn, String fileName, String tableName) throws SQLException {
		boolean local = true;
		boolean overwrite = false;
		loadData(conn, fileName, tableName, local, overwrite);
		return true;
	}
	
	public boolean loadData(Connection conn, String fileName, String tableName, boolean local, boolean overwrite) throws SQLException {
	    Statement stmt = conn.createStatement();
	    String sql = "LOAD DATA ";
	    
	    if(local == true){
	    	sql = sql + "LOCAL ";
	    }
	    sql = sql + "INPATH '" + fileName + "' ";
	    
	    if(overwrite == true) {
	    	sql = sql + "OVERWRITE ";
	    }
	    sql = sql + "INTO TABLE " + tableName + ";";
	    
	    System.out.print("Running: " + sql);
	    stmt.executeUpdate(sql);
	    stmt.close();
	    return true;
	} 
	
	// the database operations
	public boolean useDatabase(Connection conn, String database) throws SQLException {
	    Statement stmt = conn.createStatement();
	    String sql = "use " + database;
	    System.out.print("Running: " + sql);
	    stmt.executeQuery(sql);
	    stmt.close();
	    return true;
	}
	
	public boolean showTables(Connection conn) throws SQLException {
	    Statement stmt = conn.createStatement();
	    String sql = "show tables;";
	    System.out.print("Running: " + sql);
	    stmt.executeUpdate(sql);
	    stmt.close();
	    return true;
	}
	
	// the table DDL operations
	public boolean createTable(Connection conn, String theSql) throws SQLException {
		    Statement stmt = conn.createStatement();
		    String sql = theSql;
		    System.out.print("Running: " + sql);
		    stmt.executeUpdate(sql);
		    stmt.close();
		    return true;
	}
	
	public boolean dropTable(Connection conn, String tableName) throws SQLException {
	    Statement stmt = conn.createStatement();
	    String sql = "drop table " + tableName;
	    System.out.print("Running: " + sql);
	    stmt.executeUpdate(sql);
	    stmt.close();
	    return true;
	}

	public boolean describeTable(Connection conn, String tableName) throws SQLException {
	    Statement stmt = conn.createStatement();
	    String sql = "describe " + tableName;
	    System.out.print("Running: " + sql);
	    stmt.executeUpdate(sql);
	    stmt.close();
	    return true;
	}
	
	// the table DML operations
	public ResultSet executeSql(Connection conn, String theSql) throws SQLException {
	    Statement stmt = conn.createStatement();
	    ResultSet res;
	    String sql = theSql;
	    System.out.print("Running: " + sql);
	    res = stmt.executeQuery(sql);
	    stmt.close();
	    return res;
	}
	public ResultSet selectAllSql(Connection conn, String tableName) throws SQLException {
		    Statement stmt = conn.createStatement();
		    ResultSet res;
		    String sql = "select * from " + tableName;
		    System.out.print("Running: " + sql);
		    res = stmt.executeQuery(sql);
		    stmt.close();
		    return res;
	}
	
	public ResultSet selectCountSql(Connection conn, String tableName) throws SQLException {
	    Statement stmt = conn.createStatement();
	    ResultSet res;
	    String sql = "select count(*) from " + tableName;
	    System.out.print("Running: " + sql);
	    res = stmt.executeQuery(sql);
	    stmt.close();
	    return res;
	}
	
	public void printResultSet(ResultSet res) throws SQLException{
		printResultSet(res, "data");
	}
	
	public void printResultSet(ResultSet res, String col) throws SQLException{
		while (res.next()) {
			System.out.println(res.getString(col));
			System.out.print(", ");
		}
	}
}
